#!/bin/bash
# Create (or reuse) the validation EKS cluster, its gp3 StorageClass and an
# S3 gateway endpoint in its VPC. Idempotent; everything it creates goes in
# the run's ledger, and down.sh removes exactly that.
#
#   RUN=v1 REGION=us-east-1 [CLUSTER=otel-val-v1 K8S_VERSION=1.35 INSTANCE_TYPE=m6i.xlarge NODES=3 YES=1] eks/up.sh
#
# REGION defaults to us-east-1 (the prices in eks-aws.md). `consume` used to
# sign for us-east-1 whatever the bucket's region; since 2026-09-28 it takes
# --region / AWS_REGION (eks-aws.md §0), so another region needs AWS_REGION
# in the consumer's environment.
# Writes $STATE/kubeconfig; every later step uses it.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need aws eksctl kubectl python3
REGION=${REGION:-us-east-1}
CLUSTER=${CLUSTER:-otel-val-$RUN}
K8S_VERSION=${K8S_VERSION:-1.35}
INSTANCE_TYPE=${INSTANCE_TYPE:-m6i.xlarge}
NODES=${NODES:-3}
export KUBECONFIG=$STATE/kubeconfig
case "$CLUSTER" in *[!a-z0-9-]*) die "CLUSTER must be [a-z0-9-] (it is also the key segment and the ABAC tag)";; esac

render "$VALIDATION_DIR/eks/cluster.yaml.tmpl" "$STATE/cluster.yaml" CLUSTER="$CLUSTER" REGION="$REGION" \
  K8S_VERSION="$K8S_VERSION" INSTANCE_TYPE="$INSTANCE_TYPE" NODES="$NODES" NODES_MAX="$((NODES + 1))" RUN="$RUN"
if aws eks describe-cluster --region "$REGION" --name "$CLUSTER" > /dev/null 2>&1; then
  was_created eks-cluster "$CLUSTER" || log "cluster $CLUSTER exists and was not created by run $RUN: reusing it, down.sh will not delete it"
else
  confirm "create EKS cluster $CLUSTER in $REGION ($NODES x $INSTANCE_TYPE; about \$0.85/h with the NAT gateway)?" || die "not confirmed"
  created eks-cluster "$CLUSTER" "$REGION"
  eksctl create cluster -f "$STATE/cluster.yaml" || die "eksctl create cluster failed (the ledger keeps it: down.sh deletes what exists)"
fi
aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER" --kubeconfig "$KUBECONFIG" > /dev/null
kubectl get nodes -o wide

# A default gp3 StorageClass, unless the cluster has a default already (EKS
# no longer marks gp2 as default).
if [ -z "$(kubectl get sc -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{end}')" ]; then
  kubectl apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3-validation
  annotations: {storageclass.kubernetes.io/is-default-class: "true"}
  labels: {created-by: otel-chdb-validation}
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true          # the full-buffer scenario grows a PVC (fleet_test.sh growpvc)
reclaimPolicy: Delete
parameters: {type: gp3}
EOF
  created k8s-storageclass gp3-validation
fi

# S3 through a gateway endpoint: no NAT charge per GB, and the path a
# production edge would use.
VPC=$(aws eks describe-cluster --region "$REGION" --name "$CLUSTER" --query cluster.resourcesVpcConfig.vpcId --output text)
EP=$(aws ec2 describe-vpc-endpoints --region "$REGION" --filters Name=vpc-id,Values="$VPC" \
  Name=service-name,Values="com.amazonaws.$REGION.s3" Name=vpc-endpoint-type,Values=Gateway --query 'VpcEndpoints[0].VpcEndpointId' --output text)
if [ "$EP" = None ] || [ -z "$EP" ]; then
  RTS=$(aws ec2 describe-route-tables --region "$REGION" --filters Name=vpc-id,Values="$VPC" --query 'RouteTables[].RouteTableId' --output text)
  # shellcheck disable=SC2086 # RTS is a list of ids
  EP=$(aws ec2 create-vpc-endpoint --region "$REGION" --vpc-id "$VPC" --vpc-endpoint-type Gateway \
    --service-name "com.amazonaws.$REGION.s3" --route-table-ids $RTS \
    --tag-specifications "ResourceType=vpc-endpoint,Tags=[{Key=created-by,Value=otel-chdb-validation},{Key=run,Value=$RUN}]" \
    --query VpcEndpoint.VpcEndpointId --output text) || die "create-vpc-endpoint failed"
  created vpc-endpoint "$EP" "$REGION"
fi
log "cluster $CLUSTER ready; KUBECONFIG=$KUBECONFIG; S3 gateway endpoint $EP"
result eks.cluster "$CLUSTER $REGION k8s $(kubectl version -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])')"
