-- Does the central ClickHouse read the edge's objects with its OWN credentials
-- (keyless s3(): IRSA / Pod Identity / IMDS / aws_signing_helper serve), or
-- with a named collection (Nutanix Objects)?
--
-- Run as the INGEST user (the consumer's user), not as default/admin: the
-- restriction below is per user profile. Placeholders, replaced by
-- clickhouse-check.sh or by hand:
--   __URL__   the sample object left by `s3accept creds --leave-sample`
--             AWS:     https://BUCKET.s3.REGION.amazonaws.com/PREFIX/creds/sample.tsv
--             Nutanix: https://objects.example/BUCKET/PREFIX/creds/sample.tsv
--   __GLOB__  the same with *.tsv (exercises LIST, i.e. s3:ListBucket)
--
-- The 26.10 restriction: s3_allow_server_credentials_in_user_queries = 0 by
-- default on a SERVER (not in clickhouse local or chDB). A keyless s3() then
-- fails with
--   Code: 497 ... S3 access from user queries is not allowed to use the
--   server's own credentials (environment variables, instance metadata, IRSA,
--   instance profile, or AWS config files) ... (ACCESS_DENIED)
-- Fix, least privilege first:
--   a) <profiles><ingest><s3_allow_server_credentials_in_user_queries>1</...>
--      in the ingest user's profile only (and keep it 0 + readonly for others);
--   b) s3(url, 'Parquet', extra_credentials(role_arn = '...')): allowed under
--      the restriction; the role must trust the server's identity;
--   c) Nutanix / static keys: a named collection with the keys (never
--      restricted), GRANT NAMED COLLECTION ... TO ingest.
-- Note: a persistent S3/S3Queue TABLE relying on server credentials created
-- under a session-level setting becomes inaccessible after a restart; the
-- consumer uses s3() per statement, which is not affected.

-- 1. Who am I, which version, and is the restriction on for me?
SELECT version() AS version, currentUser() AS user,
       getSetting('s3_allow_server_credentials_in_user_queries') AS allow_server_creds_for_me;

-- 2. Keyless read with the server's credentials (the production path on EKS).
--    Code 497 here = the restriction (fix a or b); AccessDenied / 403 = the
--    server's role lacks s3:GetObject; timeout = no route to the credential source.
SELECT 'keyless' AS check, count() AS rows, sum(c1) AS sum_c1
FROM s3('__URL__', 'TSV', 'c1 UInt32, c2 String');

-- 3. The same with the setting lifted for this query only. If this works and
--    (2) did not, put the setting in the ingest profile (a).
SELECT 'keyless, setting=1' AS check, count() AS rows
FROM s3('__URL__', 'TSV', 'c1 UInt32, c2 String')
SETTINGS s3_allow_server_credentials_in_user_queries = 1;

-- 4. A glob (the consumer's s3('…/{k1,k2}') does no LIST, but GC tooling and
--    ad-hoc reads do): needs s3:ListBucket for the server's role.
SELECT 'glob' AS check, _path, count() AS rows
FROM s3('__GLOB__', 'TSV', 'c1 UInt32, c2 String')
GROUP BY _path
SETTINGS s3_allow_server_credentials_in_user_queries = 1;

-- 5. (AWS, optional) the role-ARN route that stays allowed under the restriction:
-- SELECT count() FROM s3('__URL__', 'TSV', 'c1 UInt32, c2 String',
--        extra_credentials(role_arn = 'arn:aws:iam::111122223333:role/otel-central-read'));

-- 6. (Nutanix) a named collection with static keys; the CA must be trusted by
--    the server: <openSSL><client><caConfig>/etc/clickhouse-server/nutanix-ca.pem</caConfig>
--    or SSL_CERT_FILE (AWS_CA_BUNDLE is ignored by ClickHouse).
-- CREATE NAMED COLLECTION nutanix_otel AS
--   url = 'https://objects.example/BUCKET/PREFIX/', access_key_id = '...', secret_access_key = '...';
-- GRANT NAMED COLLECTION ON nutanix_otel TO ingest;
-- SELECT count() FROM s3(nutanix_otel, filename = 'creds/sample.tsv', format = 'TSV', structure = 'c1 UInt32, c2 String');

-- 7. Which credential source did the server use? (server log, not SQL)
--    grep -E 'AWSClient|Credentials|WebIdentity|ECSCredentials|InstanceProfile' /var/log/clickhouse-server/clickhouse-server.log | tail
--    ClickHouse's chain: web identity (IRSA), env, SSO, container (Pod Identity), IMDS, then the
--    shared credentials file's keys. It does NOT run credential_process: for Roles Anywhere run
--    `aws_signing_helper serve` beside the server and set AWS_EC2_METADATA_SERVICE_ENDPOINT.
--    IRSA's STS endpoint is hard-coded to https://sts.<region>.amazonaws.com (AWS_ENDPOINT_URL_STS
--    is ignored); the region comes from AWS_DEFAULT_REGION or the profile, not AWS_REGION.
