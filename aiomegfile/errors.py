import os
import typing as T

from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError

from aiomegfile.smart_path import SmartPath


def s3_endpoint_url(path: T.Optional[T.Union[str, SmartPath, os.PathLike]] = None):
    from aiomegfile.filesystem.s3 import S3FileSystem, get_endpoint_url, get_s3_client

    profile_name = None
    if path:
        profile_name = S3FileSystem(path)._profile_name
    endpoint_url = get_endpoint_url(profile_name=profile_name)
    if endpoint_url is None:
        endpoint_url = get_s3_client(profile_name=profile_name).meta.endpoint_url
    return endpoint_url


def full_class_name(obj):
    # obj.__module__ + "." + obj.__class__.__qualname__ is an example in
    # this context of H.L. Mencken's "neat, plausible, and wrong."
    # Python makes no guarantees as to whether the __module__ special
    # attribute is defined, so we take a more circumspect approach.
    # Alas, the module name is explicitly excluded from __qualname__
    # in Python 3.

    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__  # Avoid reporting __builtin__
    else:
        return module + "." + obj.__class__.__name__


def full_error_message(error):
    return "%s(%r)" % (full_class_name(error), str(error))


def client_error_code(error: ClientError) -> str:
    error_data = error.response.get("Error", {})
    return error_data.get("Code") or error_data.get("code", "Unknown")


def client_error_message(error: ClientError) -> str:
    return error.response.get("Error", {}).get("Message", "Unknown")


def param_validation_error_report(error: ParamValidationError) -> str:
    return error.kwargs.get("report", "Unknown")


class UnknownError(Exception):
    def __init__(
        self,
        error: Exception,
        path: T.Union[str, SmartPath, os.PathLike],
        extra: T.Optional[str] = None,
    ):
        message = "Unknown error encountered: %r, error: %s" % (
            path,
            full_error_message(error),
        )
        if extra is not None:
            message += ", " + extra
        super().__init__(message)
        self.path = path
        self.extra = extra
        self.__cause__ = error

    def __reduce__(self):
        return (self.__class__, (self.__cause__, self.path, self.extra))


class ProtocolNotFoundError(Exception):
    pass


class S3Exception(Exception):
    """
    Base type for all s3 errors, should NOT be constructed directly.
    When you try to do so, consider adding a new type of error.
    """


class S3FileNotFoundError(S3Exception, FileNotFoundError):
    pass


class S3BucketNotFoundError(S3FileNotFoundError, PermissionError):
    pass


class S3FileExistsError(S3Exception, FileExistsError):
    pass


class S3NotADirectoryError(S3Exception, NotADirectoryError):
    pass


class S3IsADirectoryError(S3Exception, IsADirectoryError):
    pass


class S3FileChangedError(S3Exception):
    pass


class S3PermissionError(S3Exception, PermissionError):
    pass


class S3ConfigError(S3Exception, EnvironmentError):
    """
    Error raised by wrong S3 config, including wrong config file format,
    wrong aws_secret_access_key / aws_access_key_id, and etc.
    """


class S3NotALinkError(S3FileNotFoundError, PermissionError):
    pass


class S3NameTooLongError(S3FileNotFoundError, PermissionError):
    pass


class S3InvalidRangeError(S3Exception):
    pass


class S3UnknownError(S3Exception, UnknownError):
    def __init__(
        self,
        error: Exception,
        path: T.Union[str, SmartPath, os.PathLike],
        extra: T.Optional[str] = None,
    ):
        super().__init__(error, path, extra or "endpoint: %r" % s3_endpoint_url(path))


def translate_s3_error(
    s3_error: Exception,
    s3_url: T.Union[str, SmartPath, os.PathLike],
) -> Exception:
    """:param s3_error: error raised by boto3
    :param s3_url: s3_url
    """
    if isinstance(s3_error, S3Exception):
        return s3_error
    elif isinstance(s3_error, ClientError):
        code = client_error_code(s3_error)
        if code in ("NoSuchBucket"):
            bucket_or_url = (
                s3_error.response.get(  # pytype: disable=attribute-error
                    "Error", {}
                ).get("BucketName")
                or s3_url
            )
            return S3BucketNotFoundError(
                "No such bucket: %r, endpoint: %r"
                % (bucket_or_url, s3_endpoint_url(s3_url))
            )
        if code in ("404", "NoSuchKey"):
            return S3FileNotFoundError("No such file: %r" % s3_url)
        if code in ("401", "403", "AccessDenied"):
            message = client_error_message(s3_error)
            return S3PermissionError(
                "Permission denied: %r, code: %r, message: %r, endpoint: %r"
                % (s3_url, code, message, s3_endpoint_url(s3_url))
            )
        if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
            message = client_error_message(s3_error)
            return S3ConfigError(
                "Invalid configuration: %r, code: %r, message: %r, endpoint: %r"
                % (s3_url, code, message, s3_endpoint_url(s3_url))
            )
        if code in ("InvalidRange", "Requested Range Not Satisfiable"):
            return S3InvalidRangeError(
                "Index out of range: %r, code: %r, message: %r, endpoint: %r"
                % (
                    s3_url,
                    code,
                    client_error_message(s3_error),
                    s3_endpoint_url(s3_url),
                )
            )
        return S3UnknownError(s3_error, s3_url)
    elif isinstance(s3_error, ParamValidationError):
        report = param_validation_error_report(s3_error)
        if "Invalid bucket name" in report:
            return S3BucketNotFoundError("Invalid bucket name: %r" % s3_url)
        if "Invalid length for parameter Key" in report:
            return S3FileNotFoundError("Invalid length for parameter Key: %r" % s3_url)
        return S3UnknownError(s3_error, s3_url)
    elif isinstance(s3_error, NoCredentialsError):
        return S3ConfigError(str(s3_error))
    # elif isinstance(s3_error, (S3UploadFailedError, S3TransferFailedError)):
    #     if "NoSuchBucket" in str(s3_error):
    #         return S3BucketNotFoundError("No such bucket: %r" % s3_url)
    #     elif "NoSuchKey" in str(s3_error):
    #         return S3FileNotFoundError("No such file: %r" % s3_url)
    #     elif "InvalidAccessKeyId" in str(s3_error) or "SignatureDoesNotMatch" in str(
    #         s3_error
    #     ):
    #         return S3ConfigError("Invalid access key id: %r" % s3_url)
    #     elif "InvalidRange" in str(s3_error):
    #         return S3InvalidRangeError("Invalid range: %r" % s3_url)
    #     elif "AccessDenied" in str(s3_error):
    #         return S3PermissionError("Access denied: %r" % s3_url)
    return S3UnknownError(s3_error, s3_url)
