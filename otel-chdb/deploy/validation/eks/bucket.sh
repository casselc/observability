#!/bin/bash
# The validation bucket. Creates it if missing (tagged, public access blocked,
# a lifecycle rule that expires validation/ after EXPIRE_DAYS as a safety
# net); reuses it unchanged if it exists. Never deletes a bucket, and never
# replaces an existing bucket's lifecycle or policy.
#
#   RUN=v1 BUCKET=my-otel-validation REGION=us-east-1 [EXPIRE_DAYS=7] eks/bucket.sh
#
# The format-v2 layout needs nothing created in advance: every key is
# {S3_BASE}/{cluster}/{producer}/{signal}/{epoch}/{seq}.parquet under
# S3_BASE = s3://$BUCKET/$VPREFIX/edge (FORMAT.md §1), control objects under
# .../edge/_consumer/, entity lanes under $VPREFIX/entities/.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need aws
: "${BUCKET:?}"
REGION=${REGION:-us-east-1}
EXPIRE_DAYS=${EXPIRE_DAYS:-7}
if awss3 s3api head-bucket --bucket "$BUCKET" 2> /dev/null; then
  tag=$(awss3 s3api get-bucket-tagging --bucket "$BUCKET" --query "TagSet[?Key=='created-by'].Value" --output text 2> /dev/null || true)
  log "bucket $BUCKET exists (created-by: ${tag:-none}); left as it is"
else
  confirm "create bucket $BUCKET in $REGION?" || die "not confirmed"
  if [ "$REGION" = us-east-1 ]; then
    awss3 s3api create-bucket --bucket "$BUCKET" --region "$REGION" > /dev/null
  else
    awss3 s3api create-bucket --bucket "$BUCKET" --region "$REGION" --create-bucket-configuration "LocationConstraint=$REGION" > /dev/null
  fi || die "create-bucket failed"
  created bucket "$BUCKET" "$REGION"
  awss3 s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  awss3 s3api put-bucket-tagging --bucket "$BUCKET" --tagging "TagSet=[{Key=created-by,Value=otel-chdb-validation},{Key=run,Value=$RUN}]"
  awss3 s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration "{\"Rules\":[{\"ID\":\"expire-validation\",\"Status\":\"Enabled\",\"Filter\":{\"Prefix\":\"validation/\"},\"Expiration\":{\"Days\":$EXPIRE_DAYS},\"AbortIncompleteMultipartUpload\":{\"DaysAfterInitiation\":1}}]}"
fi
# What the store has, for the record (versioning matters to GC: a delete on a
# versioned bucket keeps the bytes).
result bucket.versioning "$(awss3 s3api get-bucket-versioning --bucket "$BUCKET" --query Status --output text 2> /dev/null)"
result bucket.encryption "$(awss3 s3api get-bucket-encryption --bucket "$BUCKET" --query 'ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm' --output text 2> /dev/null)"
result bucket.policy "$(awss3 s3api get-bucket-policy --bucket "$BUCKET" > /dev/null 2>&1 && echo present || echo none)"
log "S3_BASE=s3://$BUCKET/$VPREFIX/edge"
