__author__ = "Christopher Tomkins-Tinch"
__copyright__ = "Copyright 2015, Christopher Tomkins-Tinch"
__email__ = "tomkinsc@broadinstitute.org"
__license__ = "MIT"

# built-ins
import os
import re
import math
import functools
import concurrent.futures

# module-specific
from snakemake.remote import AbstractRemoteObject, AbstractRemoteProvider
from snakemake.exceptions import WorkflowError, S3FileException
from snakemake.utils import os_sync

from snakemake.logging import logger

try:
    # third-party modules
    import boto3
    import botocore
except ImportError as e:
    raise WorkflowError(
        "The Python 3 package 'boto3' "
        "needs to be installed to use S3 remote() file functionality. %s" % e.msg
    )


class RemoteProvider(
    AbstractRemoteProvider
):  # class inherits from AbstractRemoteProvider

    supports_default = True  # class variable
    provider_name = "S3"

    @classmethod
    def update_cache(cls, bucket_name, key, data):
        import time

        s3_cache = cls.cache[cls.provider_name]
        if bucket_name not in s3_cache:
            s3_cache[bucket_name] = {}

        s3_cache[bucket_name][key] = {"data": data, "cache_ts": time.time()}


    @classmethod
    def invalidate_cache(cls):
        # For example, right prior to starting a Kubernetes pod, it's vital to
        # invalidate the cache. By that time, we have already built the workflow
        # DAG, the scheduler and wait for job to complete and then check the
        # output
        cls.cache[cls.provider_name] = {}


    @classmethod
    def retrieve_cache(cls, bucket_name, key, cache_ttl, func=None):
        import time

        # Try retrieve cache entry
        s3_cache = cls.cache[cls.provider_name]
        # Cache for this particular bucket
        bucket_cache = s3_cache.get(bucket_name, {})
        s3_cache[bucket_name] = bucket_cache
        if key in bucket_cache:
            contents = bucket_cache[key]
            current_ts = time.time()
            if current_ts - contents["cache_ts"] < cache_ttl:
                return contents["data"]
        
        if func is not None:
            # Cache missed. Try retrieving the data and update
            data = func()
            cls.update_cache(bucket_name, key, data)
            return data

    def __init__(
        self, *args, keep_local=False, stay_on_remote=False, is_default=False, 
        enable_cache=True, cache_ttl=30,
        **kwargs
    ):
        super(RemoteProvider, self).__init__(
            *args,
            keep_local=keep_local,
            stay_on_remote=stay_on_remote,
            is_default=is_default,
            enable_cache=enable_cache,
            provider_name=self.provider_name,
            cache_ttl=cache_ttl,
            **kwargs
        )  # in addition to methods provided by AbstractRemoteProvider, we add these in

        self._s3c = S3Helper(*args, **kwargs)  # _private variable by convention

    def remote_interface(self):
        return self._s3c

    @property  # decorator, so that this function can be access as an attribute, instead of a method
    def default_protocol(self):
        """The protocol that is prepended to the path when no protocol is specified."""
        return "s3://"

    @property  # decorator, so that this function can be access as an attribute, instead of a method
    def available_protocols(self):
        """List of valid protocols for this remote provider."""
        return ["s3://"]


class RemoteObject(AbstractRemoteObject):
    """This is a class to interact with the AWS S3 object store."""

    def __init__(self, *args, keep_local=False, provider=None, **kwargs):
        super(RemoteObject, self).__init__(
            *args, keep_local=keep_local, provider=provider, **kwargs
        )

        if provider:
            self._s3c = provider.remote_interface()
        else:
            self._s3c = S3Helper(*args, **kwargs)

    # === Implementations of abstract class members ===

    # Helper function for mtime() and size()
    def _retrieve_cache_helper(self, prop, retry=True):
        provider = self.provider

        for _ in range(3 if retry else 2):
            results = type(provider).retrieve_cache(self.s3_bucket, self.s3_key, provider.cache_ttl)
            if results is None:
                # Cache missing. Regenerate the entire bucket list
                self.list
                continue

            return results[prop]
            
        raise WorkflowError(f"Failed to retrieve cache: {self.s3_bucket}/{self.s3_key}")


    def exists(self):
        if self._matched_s3_path:
            provider = self.provider
            if provider.enable_cache:
                cache_entry = self.provider.retrieve_cache(self.s3_bucket, self.s3_key, provider.cache_ttl, \
                    lambda: self.list_bucket(force_refresh=True).get(self.s3_key))
                    
                return False if cache_entry is None else True
            else:
                return self._s3c.exists_in_bucket(self.s3_bucket, self.s3_key)
        else:
            raise S3FileException(
                "The file cannot be parsed as an s3 path in form 'bucket/key': %s"
                % self.local_file()
            )

    def mtime(self):
        if self.exists():
            provider = self.provider
            if provider.enable_cache:
                return self._retrieve_cache_helper("mtime")
            else:
                return self._s3c.key_last_modified(self.s3_bucket, self.s3_key)
        else:
            raise S3FileException(
                "The file does not seem to exist remotely: %s" % self.local_file()
            )

    def size(self):
        if self.exists():
            provider = self.provider
            if provider.enable_cache:
                return self._retrieve_cache_helper("size") // 1024
            else:
                return self._s3c.key_size(self.s3_bucket, self.s3_key)
        else:
            return self._iofile.size_local

    def download(self):
        self._s3c.download_from_s3(self.s3_bucket, self.s3_key, self.local_file())
        os_sync()  # ensure flush to disk

    def upload(self):
        self._s3c.upload_to_s3(
            self.s3_bucket,
            self.local_file(),
            self.s3_key,
            extra_args=self.kwargs.get("ExtraArgs", None),
            config=self.kwargs.get("Config", None),
        )

        if self.provider.enable_cache:
            # Update the cache
            size = self._s3c.key_size(self.s3_bucket, self.s3_key, size_in_kb=False)
            mtime = self._s3c.key_last_modified(self.s3_bucket, self.s3_key)
            self.provider.update_cache(self.s3_bucket, self.s3_key, {"mtime": mtime, "size": size})

    def list_bucket(self, force_refresh=False):
        provider = self.provider
        assert provider.enable_cache is True

        import time
        current_ts = time.time()

        if force_refresh:
            logger.debug(f"Force-refresh the cache: {self.s3_bucket}...")
            # bucket_objects: [key, size, mtime]
            bucket_objects = self._s3c.list_bucket(self.s3_bucket)
            update_contents = {k: {"cache_ts": current_ts, "data": v} for k, v in bucket_objects.items()}
            # Update the cache
            type(provider).cache[provider.provider_name][self.s3_bucket] = update_contents
            return bucket_objects

        cache_contents = type(provider).cache[provider.provider_name].get(self.s3_bucket, {})

        # Find the cache_ts and determine whether to refresh the cache
        cache_ts = list(cache_contents.values())[0]["cache_ts"] if cache_contents else 0
        if current_ts - cache_ts < provider.cache_ttl:
            # List of keys
            return cache_contents
        else:
            logger.debug(f"Cache missed. Refresh the cache: {self.s3_bucket}...")
            # bucket_objects: [key, size, mtime]
            bucket_objects = self._s3c.list_bucket(self.s3_bucket)
            update_contents = {k: {"cache_ts": current_ts, "data": v} for k, v in bucket_objects.items()}
            # Update the cache
            type(provider).cache[provider.provider_name][bucket_name] = update_contents
            return bucket_objects

    @property
    def list(self):
        provider = self.provider
        bucket_name = self.s3_bucket
        if provider.enable_cache:
            return self.list_bucket().keys()
        else:
            # Not using cache and list the bucket on the fly.
            return self._s3c.list_keys(bucket_name)

    # === Related methods ===

    @property
    def _matched_s3_path(self):
        return re.search("(?P<bucket>[^/]*)/(?P<key>.*)", self.local_file())

    @property
    def s3_bucket(self):
        if len(self._matched_s3_path.groups()) == 2:
            return self._matched_s3_path.group("bucket")
        return None

    @property
    def name(self):
        return self.s3_key

    @property
    def s3_key(self):
        if len(self._matched_s3_path.groups()) == 2:
            return self._matched_s3_path.group("key")

    def s3_create_stub(self):
        if self._matched_s3_path:
            if not self.exists:
                self._s3c.download_from_s3(
                    self.s3_bucket, self.s3_key, self.file, create_stub_only=True
                )
        else:
            raise S3FileException(
                "The file to be downloaded cannot be parsed as an s3 path in form 'bucket/key': %s"
                % self.local_file()
            )


class S3Helper(object):
    def __init__(self, *args, **kwargs):
        # as per boto, expects the environment variables to be set:
        # AWS_ACCESS_KEY_ID
        # AWS_SECRET_ACCESS_KEY
        # Otherwise these values need to be passed in as kwargs

        # allow key_id and secret to be specified with aws_, gs_, or no prefix.
        # Standardize to the aws_ prefix expected by boto.
        if "gs_access_key_id" in kwargs:
            kwargs["aws_access_key_id"] = kwargs.pop("gs_access_key_id")
        if "gs_secret_access_key" in kwargs:
            kwargs["aws_secret_access_key"] = kwargs.pop("gs_secret_access_key")
        if "access_key_id" in kwargs:
            kwargs["aws_access_key_id"] = kwargs.pop("access_key_id")
        if "secret_access_key" in kwargs:
            kwargs["aws_secret_access_key"] = kwargs.pop("secret_access_key")
        if "host" in kwargs:
            kwargs["endpoint_url"] = kwargs.pop("host")

        self.s3 = boto3.resource("s3", **kwargs)

    def bucket_exists(self, bucket_name):
        try:
            self.s3.meta.client.head_bucket(Bucket=bucket_name)
            return True
        except:
            return False

    def upload_to_s3(
        self,
        bucket_name,
        file_path,
        key=None,
        use_relative_path_for_key=True,
        relative_start_dir=None,
        extra_args=None,
        config=None,
    ):
        """Upload a file to S3

        This function uploads a file to an AWS S3 bucket.

        Args:
            bucket_name: the name of the S3 bucket to use (bucket name only, not ARN)
            file_path: The path to the file to upload.
            key: The key to set for the file on S3. If not specified, this will default to the
                name of the file.
            use_relative_path_for_key: If set to True (default), and key is None, the S3 key will include slashes
                representing the path of the file relative to the CWD. If False only the
                file basename will be used for the key.
            relative_start_dir: The start dir to use for use_relative_path_for_key. No effect if key is set.

        Returns: The key of the file on S3 if written, None otherwise
        """
        file_path = os.path.realpath(os.path.expanduser(file_path))

        assert bucket_name, "bucket_name must be specified"
        assert os.path.exists(file_path), (
            "The file path specified does not exist: %s" % file_path
        )
        assert os.path.isfile(file_path), (
            "The file path specified does not appear to be a file: %s" % file_path
        )

        if not self.bucket_exists(bucket_name):
            self.s3.create_bucket(Bucket=bucket_name)

        if not key:
            if use_relative_path_for_key:
                if relative_start_dir:
                    path_key = os.path.relpath(file_path, relative_start_dir)
                else:
                    path_key = os.path.relpath(file_path)
            else:
                path_key = os.path.basename(file_path)
            key = path_key

        k = self.s3.Object(bucket_name, key)

        try:
            k.upload_file(file_path, ExtraArgs=extra_args, Config=config)
        except:
            raise

    def download_from_s3(
        self,
        bucket_name,
        key,
        destination_path=None,
        expandKeyIntoDirs=True,
        make_dest_dirs=True,
        create_stub_only=False,
    ):
        """Download a file from s3

        This function downloads an object from a specified AWS S3 bucket.

        Args:
            bucket_name: the name of the S3 bucket to use (bucket name only, not ARN)
            destination_path: If specified, the file will be saved to this path, otherwise cwd.
            expandKeyIntoDirs: Since S3 keys can include slashes, if this is True (defult)
                then S3 keys with slashes are expanded into directories on the receiving end.
                If it is False, the key is passed to os.path.basename() to get the substring
                following the last slash.
            make_dest_dirs: If this is True (default) and the destination path includes directories
                that do not exist, they will be created.

        Returns:
            The destination path of the downloaded file on the receiving end, or None if the destination_path
            could not be downloaded
        """
        assert bucket_name, "bucket_name must be specified"
        assert key, "Key must be specified"

        if destination_path:
            destination_path = os.path.realpath(os.path.expanduser(destination_path))
        else:
            if expandKeyIntoDirs:
                destination_path = os.path.join(os.getcwd(), key)
            else:
                destination_path = os.path.join(os.getcwd(), os.path.basename(key))

        # if the destination path does not exist
        if make_dest_dirs:
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        k = self.s3.Object(bucket_name, key)

        try:
            if not create_stub_only:
                k.download_file(destination_path)
            else:
                # just create an empty file with the right timestamps
                with open(destination_path, "wb") as fp:
                    os.utime(
                        fp.name,
                        (k.last_modified.timestamp(), k.last_modified.timestamp()),
                    )
            return destination_path
        except:
            raise S3FileException(
                "Error downloading file '%s' from bucket '%s'." % (key, bucket_name)
            )

    def delete_from_bucket(self, bucket_name, key):
        """Delete a file from s3

        This function deletes an object from a specified AWS S3 bucket.

        Args:
            bucket_name: the name of the S3 bucket to use (bucket name only, not ARN)
            key: the key of the object to delete from the bucket

        Returns:
            The name of the object deleted
        """
        assert bucket_name, "bucket_name must be specified"
        assert key, "Key must be specified"

        k = self.s3.Object(bucket_name, key)
        ret = k.delete()
        return ret.name

    def exists_in_bucket(self, bucket_name, key):
        """Returns whether the key exists in the bucket

        Args:
            bucket_name: the name of the S3 bucket to use (bucket name only, not ARN)
            key: the key of the object to delete from the bucket

        Returns:
            True | False
        """
        assert bucket_name, "bucket_name must be specified"
        assert key, "Key must be specified"

        try:
            self.s3.Object(bucket_name, key).load()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                raise
        return True

    def key_size(self, bucket_name, key, size_in_kb=True):
        """Returns the size of a key based on a HEAD request

        Args:
            bucket_name: the name of the S3 bucket to use (bucket name only, not ARN)
            key: the key of the object to delete from the bucket
            size_in_kb: if True, return size in kilobytes, otherwise in bytes

        Returns:
            Size in kb
        """
        assert bucket_name, "bucket_name must be specified"
        assert key, "Key must be specified"

        k = self.s3.Object(bucket_name, key)

        if size_in_kb:
            return k.content_length // 1024
        else:
            return k.content_length

    def key_last_modified(self, bucket_name, key):
        """Returns a timestamp of a key based on a HEAD request

        Args:
            bucket_name: the name of the S3 bucket to use (bucket name only, not ARN)
            key: the key of the object to delete from the bucket

        Returns:
            timestamp
        """
        assert bucket_name, "bucket_name must be specified"
        assert key, "Key must be specified"

        k = self.s3.Object(bucket_name, key)

        return k.last_modified.timestamp()

    def list_keys(self, bucket_name):
        b = self.s3.Bucket(bucket_name)
        return [o.key for o in b.objects.iterator()]

    def list_bucket(self, bucket_name):
        # Unlike list_keys, this method lists all objcts in the bucket, with the key, size and 
        # last modified time
        b = self.s3.Bucket(bucket_name)
        bucket_objects = {o.key: {"mtime": o.last_modified.timestamp(), "size": o.size} \
            for o in b.objects.iterator()}
        return bucket_objects
