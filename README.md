# s3-sftp-proxy

`s3-sftp-proxy` is a tiny program that exposes the resources on your AWS S3 buckets through SFTP protocol.

## Usage

```
Usage of s3-sftp-proxy:
  -bind string
        listen on addr:port
  -config string
        configuration file (default "s3-sftp-proxy.toml")
  -debug
        turn on debugging output
```

* `-bind`

	Specifies the local address and port to listen on.  This overrides the value of `bind` in the configuration file.  If it is not present in the configuration file either, it defaults to `:10022`.

* `-config`

	Specifies the path to the configuration file.  It defaults to "./s3-sftp-config.toml" if not given.

* `-debug`

	Turn on debug logging.  The output will be more verbose.


## Configuation

The configuration file is in [TOML](https://github.com/toml-lang/toml) format.  Refer to that page for the detailed explanation of the syntax.

### Top level

```toml
host_key_file = "./host_key"
bind = "localhost:10022"
banner = """
Welcome to my SFTP server
"""
reader_lookback_buffer_size = 1048576
reader_min_chunk_size = 262144
lister_lookback_buffer_size = 100
metrics_bind = ":2112"
metrics_endpoint = "/metrics"

# buckets and authantication settings follow...
```

* `host_key_file` (required)

	Specifies the path to the host key file (private key).

	The host key can be generated with `ssh-keygen` command:

	```sh
	ssh-keygen -f host_key
	```

* `bind` (optional, defaults to `":10022"`)

	Specifies the local address and port to listen on.

* `banner` (optional, defaults to an empty string)

	A banner is a message text that will be sent to the client when the connection is esablished to the server prior to any authentication steps.

* `reader_lookback_buffer_size` (optional, defaults to `1048576`)

	Specifies the size of the buffer used to keep several amounts of data read from S3 for later access to it.  The reason why such buffer is necessary is that SFTP protocol requires the data should be sent or retrieved on a random-access basis (i.e. each request contains an offset) while those coming from S3 is actually fetched in a streaming manner.   In that we have to emulate block storage access for S3 objects, but chances are we don't need to hold the entire data with the reasonable SFTP clients.

* `reader_min_chunk_size` (optional, defaults to `262144`)

	Specifies the amount of data fetched from S3 at once.  Increase the value when you experience quite a poor performance.

* `lister_lookback_buffer_size` (optional, defalts to `100`)

	Contrary to the people's expectation, SFTP also requires file listings to be retrieved in random-access as well.

* `buckets` (required)

	`buckets` contains records for bucket declarations.  See [Bucket Settings](#bucket-settings) for detail.

* `auth`

	`auth` contains records for authenticator configurations.  See [Authenticator Settings](#authenticator-settings) for detail.

### Bucket Settings

```toml
[buckets.test]
endpoint = "http://endpoint"
s3_force_path_style = true
disable_ssl = false
bucket = "BUCKET"
key_prefix = "PREFIX"
bucket_url = "s3://BUCKET/PREFIX"
profile = "profile"
region = "ap-northeast-1"
max_object_size = 65536
writable = false
readable = true
listable = true
auth = "test"
server_side_encryption = "kms"
sse_customer_key = ""
sse_kms_key_id = ""
keyboard_interactive_auth = false

[buckets.test.credentials]
aws_access_key_id = "aaa"
aws_secret_access_key = "bbb"
```

* `endpoint` (optional)
	Specifies s3 endpoint (server) different from AWS.

* `s3_force_path_style` (optional)
    This option should be set to `true` if ypu use endpount different from AWS.

	Set this to `true` to force the request to use path-style addressing, i.e., `http://s3.amazonaws.com/BUCKET/KEY`. By default, the S3 client will use virtual hosted bucket addressing when possible (`http://BUCKET.s3.amazonaws.com/KEY`).

* `disable_ssl` (optional)
	Set this to `true` to disable SSL when sending requests.

* `bucket` (required when `bucket_url` is unspecified)

	Specifies the bucket name.

* `key_prefix` (required when `bucket_url` is unspecified)

	Specifies the prefix prepended to the file path sent from the client.  The key string is derived as follows:

		`key` = `key_prefix` + `path`

* `bucket_url` (required when `bucket` is unspecified)

	Specifies both the bucket name and prefix in the URL form.  The URL's scheme must be `s3`, and the host part corresponds to `bucket` while the path part does to `key_prefix`.  You may not specify `bucket_url` and either `bucket` or `key_prefix` at the same time.

* `profile` (optional, defaults to the value of `AWS_PROFILE` unless `credentials` is specified)

    Specifies the credentials profile name.

* `region` (optional, defaults to the value of `AWS_REGION` environment variable)

    Specifies the region of the endpoint.

* `credentials` (optional)

    * `credentials.aws_access_key_id` (required)

        Specifies the AWS access key.

    * `credentials.aws_secret_access_key` (required)

        Specifies the AWS secret access key.

* `max_object_size` (optional, defaults to unlimited)

	Specifies the maximum size of an object put to S3.  This actually sets the size of the in-memory buffer used to hold the entire content sent from the client, as we have to calculate a MD5 sum for it before uploading there.

* `readable` (optional, defaults to `true`)

	Specifies whether to allow the client to fetch objects from S3.

* `writable` (optional, defaults to `true`)

	Specifies whether to allow the client to put objects to S3.

* `listable` (optional, defaults to `true`)

	Specifies whether to allow the client to list objects in S3.

* `server_side_encryption` (optional, defaults to `"none"`)

	Specifies which server-side encryption scheme is applied to store the objects.  Valid values are: `"aes256"` and `"kms"`.

* `sse_customer_key` (required when `server_side_encryption` is set to `"aes256"`)

	Specifies the base64-encoded encryption key.  As the cipher is AES256-CBC, the key must be 256-bits long (32 bytes)

* `sse_kms_key_id` (required when `server_side_encryption` is est to `"kms"`)

	Specifies the CMK ID used for the server-side encryption using KMS.

* `keyboard_interactive_auth` (optional, defaults to `false`)

    Enables keyboard interactive authentication if set to true.

* `auth` (required)

    Specifies the name of the authenticator.


### Authenticator Settings

```toml
[auth.test]
type = "inplace"

# authenticator specific settings follow
```

* `type` (required)

    Specifies the authenticator implementation type.  Currently `"inplace"` is the only valid value.

* `users` (required when `type` is `"inplace"`)

    Contains user records as a dictionary.


#### In-place authenticator

In-place authenticator reads the credentials directly embedded in the configuration file.  The user record looks like the following:

```toml
[auth.test]
type = "inplace"

[auth.test.users.user0]
password = "test"
public_keys = """
ssh-rsa AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
ssh-rsa AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
"""

[auth.test.users.user1]
password = "test"
public_keys = """
ssh-rsa AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
ssh-rsa AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
"""
```

Or

```toml
[auth.test]
type = "inplace"

[auth.test.users]
user0 = { password="test", public_keys="..." }
user1 = { password="test", public_keys="..." }
```

* (key) (appears as `user0` or `user1` in the above example)

    Specifies the name of the user.

* `password` (optional)

    Specifies the password in a clear-text form.

* `public_keys` (optional)

    Specifies the public keys authorized to use in authentication.  Multiple keys can be specified by delimiting them by newlines.

### Prometheus metrics

* `sftp_operation_status` _(counter)_

    Represents SFTP operation statuses count by method

* `sftp_aws_session_error` _(counter)_

    AWS S3 session errors count

* `sftp_permissions_error` _(counter)_

    Bucket permission errors count by method
