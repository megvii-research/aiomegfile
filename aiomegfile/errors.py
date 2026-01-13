from contextlib import contextmanager

from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError

from aiomegfile.pathlike import PathLike


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


class ProtocolNotFoundError(Exception): ...


class UnknownError(Exception):
    def __init__(self, error: Exception, path: PathLike, extra: str | None = None):
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


class S3Exception(Exception):
    """
    Base type for all s3 errors, should NOT be constructed directly.
    When you try to do so, consider adding a new type of error.
    """


class S3FileNotFoundError(S3Exception, FileNotFoundError): ...


class S3BucketNotFoundError(S3FileNotFoundError, PermissionError): ...


class S3FileExistsError(S3Exception, FileExistsError): ...


class S3NotADirectoryError(S3Exception, NotADirectoryError): ...


class S3IsADirectoryError(S3Exception, IsADirectoryError): ...


class S3FileChangedError(S3Exception): ...


class S3PermissionError(S3Exception, PermissionError): ...


class S3ConfigError(S3Exception, EnvironmentError):
    """
    Error raised by wrong S3 config, including wrong config file format,
    wrong aws_secret_access_key / aws_access_key_id, and etc.
    """


class S3NotALinkError(S3FileNotFoundError, PermissionError): ...


class S3NameTooLongError(S3FileNotFoundError, PermissionError): ...


class S3InvalidRangeError(S3Exception): ...


class S3UnknownError(S3Exception, UnknownError):
    def __init__(self, error: Exception, path: PathLike, extra: str | None = None):
        super().__init__(error, path, extra)


def client_error_code(error: ClientError) -> str:
    error_data = error.response.get("Error", {})
    return error_data.get("Code") or error_data.get("code", "Unknown")


def client_error_message(error: ClientError) -> str:
    return error.response.get("Error", {}).get("Message", "Unknown")


def param_validation_error_report(error: ParamValidationError) -> str:
    return error.kwargs.get("report", "Unknown")


def translate_s3_error(s3_error: Exception, s3_url: PathLike) -> Exception:
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
            return S3BucketNotFoundError(f"No such bucket: {bucket_or_url!r}")
        if code in ("404", "NoSuchKey"):
            return S3FileNotFoundError("No such file: %r" % s3_url)
        if code in ("401", "403", "AccessDenied"):
            message = client_error_message(s3_error)
            return S3PermissionError(
                f"Permission denied: {s3_url!r}, code: {code}, message: {message!r}"
            )
        if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
            message = client_error_message(s3_error)
            return S3ConfigError(
                f"Invalid configuration: {s3_url!r}, code: {code}, message: {message!r}"
            )
        if code in ("InvalidRange", "Requested Range Not Satisfiable"):
            return S3InvalidRangeError(
                f"Invalid range: {s3_url!r}, code: {code}, "
                f"message: {client_error_message(s3_error)!r}"
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
    return S3UnknownError(s3_error, s3_url)


@contextmanager
def raise_s3_error(s3_url: PathLike, suppress_error_callback=None):
    try:
        yield
    except Exception as error:
        error = translate_s3_error(error, s3_url)
        if suppress_error_callback and suppress_error_callback(error):
            return
        raise error
