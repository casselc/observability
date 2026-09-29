package grants

import (
	"encoding/json"
	"fmt"
	"sort"
)

// IAM documents, per environment (one bucket each, research/grants.md §5):
//
//   - presign-{env}.json: the role the query service assumes, per plan and
//     cluster, with session tags env and cluster, to presign GETs. Its
//     policy reads only ROOT/${aws:PrincipalTag/cluster}/*, so a URL for
//     another cluster's key is signed but refused by S3.
//   - presign-{env}-trust.json: only the query service may assume it, only
//     with those two tags, and only with a cluster of this environment
//     that some plan grant reaches.
//   - edge-{env}.json: the tag-bound write (`resource in principal.cluster`):
//     D18's edge policy in this environment's bucket, plus a Deny for a
//     session whose cluster is not one of this environment's.
//   - write-{group}-{env}.json: a literal cluster write.
//
// The bucket and root are the registry's; ACCOUNT and QUERY_SERVICE_ROLE
// are placeholders as in deploy/iam/.

type statement map[string]any

type document struct {
	Version   string      `json:"Version"`
	Statement []statement `json:"Statement"`
}

func doc(sts ...statement) document { return document{Version: "2012-10-17", Statement: sts} }

func listPair(sid, bucket string, prefixes []string) []statement {
	arn := "arn:aws:s3:::" + bucket
	return []statement{
		{"Sid": sid, "Effect": "Allow", "Action": "s3:ListBucket", "Resource": arn,
			"Condition": map[string]any{"StringLike": map[string]any{"s3:prefix": prefixes}}},
		{"Sid": sid + "HeadCarriesNoPrefix", "Effect": "Allow", "Action": "s3:ListBucket", "Resource": arn,
			"Condition": map[string]any{"StringLikeIfExists": map[string]any{"s3:prefix": prefixes}}},
	}
}

// IAM returns the documents for out, by file name.
func IAM(out *Output, reg *Registry) (map[string]any, error) {
	files := map[string]any{}
	for _, e := range reg.envNames() {
		env := reg.Envs[e]
		bucket, root := env.Bucket, env.Root
		obj := "arn:aws:s3:::" + bucket + "/"
		// presign: the clusters some plan grant reaches
		planCl := map[string]bool{}
		for _, ts := range out.Query[e] {
			for _, t := range ts {
				if t.Role != ActPlan {
					continue
				}
				if t.Cluster == "*" {
					for _, c := range env.clusterNames() {
						planCl[c] = true
					}
				} else {
					planCl[t.Cluster] = true
				}
			}
		}
		if len(planCl) > 0 {
			tag := "${aws:PrincipalTag/cluster}"
			sts := []statement{
				{"Sid": "ReadOneClusterPerSession", "Effect": "Allow", "Action": "s3:GetObject", "Resource": obj + root + "/" + tag + "/*"},
			}
			sts = append(sts, listPair("ListOneClusterPerSession", bucket, []string{root + "/" + tag + "/*", root + "/" + tag})...)
			sts = append(sts,
				statement{"Sid": "NoControlObjects", "Effect": "Deny", "Action": "s3:GetObject", "Resource": obj + root + "/_*"},
				statement{"Sid": "ReadOnly", "Effect": "Deny", "Action": []string{"s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutObjectAcl", "s3:PutBucketPolicy"},
					"Resource": []string{"arn:aws:s3:::" + bucket, obj + "*"}},
				statement{"Sid": "NoSessionWithoutAClusterTag", "Effect": "Deny", "Action": "s3:*", "Resource": "*",
					"Condition": map[string]any{"Null": map[string]any{"aws:PrincipalTag/cluster": "true"}}},
				statement{"Sid": "NoSessionOfAnotherEnvironment", "Effect": "Deny", "Action": "s3:*", "Resource": "*",
					"Condition": map[string]any{"StringNotEquals": map[string]any{"aws:PrincipalTag/env": e}}},
			)
			files["presign-"+e+".json"] = doc(sts...)
			cls := keys(planCl)
			files["presign-"+e+"-trust.json"] = doc(statement{
				"Sid": "QueryServiceOnlyOneClusterOfThisEnvironment", "Effect": "Allow",
				"Principal": map[string]any{"AWS": "arn:aws:iam::ACCOUNT:role/QUERY_SERVICE_ROLE"},
				"Action":    []string{"sts:AssumeRole", "sts:TagSession"},
				"Condition": map[string]any{
					"StringEquals":              map[string]any{"aws:RequestTag/env": e, "aws:RequestTag/cluster": cls},
					"ForAllValues:StringEquals": map[string]any{"aws:TagKeys": []string{"env", "cluster"}},
					"Null":                      map[string]any{"aws:RequestTag/cluster": "false"},
				},
			})
		}
		// tag-bound writes
		if len(out.TagWrites[e]) > 0 {
			tagKey := env.tag()
			tag := "${aws:PrincipalTag/" + tagKey + "}"
			sts := []statement{
				{"Sid": "CreateSlotsUnderOwnClusterOnly", "Effect": "Allow", "Action": []string{"s3:PutObject", "s3:GetObject"}, "Resource": obj + root + "/" + tag + "/*"},
			}
			sts = append(sts, listPair("ListOwnCluster", bucket, []string{root + "/" + tag + "/*", root + "/" + tag})...)
			sts = append(sts, writeDenies(e, bucket, root, tagKey, env.clusterNames())...)
			files["edge-"+e+".json"] = doc(sts...)
		}
		// literal writes
		groups := out.LiteralWrites[e]
		for _, g := range keys(toSet(groups)) {
			var res, pre []string
			for _, c := range groups[g] {
				res = append(res, obj+root+"/"+c+"/*")
				pre = append(pre, root+"/"+c+"/*", root+"/"+c)
			}
			sts := []statement{{"Sid": "CreateSlotsUnderGrantedClusters", "Effect": "Allow", "Action": []string{"s3:PutObject", "s3:GetObject"}, "Resource": res}}
			sts = append(sts, listPair("ListGrantedClusters", bucket, pre)...)
			sts = append(sts, writeDenies(e, bucket, root, "", nil)...)
			files[fmt.Sprintf("write-%s-%s.json", safe(g), e)] = doc(sts...)
		}
	}
	return files, nil
}

func writeDenies(env, bucket, root, tagKey string, clusters []string) []statement {
	obj := "arn:aws:s3:::" + bucket + "/"
	sts := []statement{
		{"Sid": "NeverDeleteNeverControl", "Effect": "Deny", "Action": []string{"s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutObjectAcl", "s3:PutBucketPolicy"},
			"Resource": []string{"arn:aws:s3:::" + bucket, obj + "*"}},
		{"Sid": "NoControlPrefix", "Effect": "Deny", "Action": "s3:PutObject", "Resource": obj + root + "/_*"},
		{"Sid": "NoIndexOrOtherControlPrefixInsideTheCluster", "Effect": "Deny", "Action": "s3:PutObject", "Resource": obj + root + "/*/_*"},
	}
	if tagKey != "" {
		sts = append(sts,
			statement{"Sid": "NoSessionWithoutAClusterTag", "Effect": "Deny", "Action": "s3:*", "Resource": "*",
				"Condition": map[string]any{"Null": map[string]any{"aws:PrincipalTag/" + tagKey: "true"}}},
			statement{"Sid": "NoClusterOfAnotherEnvironment", "Effect": "Deny", "Action": "s3:*", "Resource": "*",
				"Condition": map[string]any{"StringNotEquals": map[string]any{"aws:PrincipalTag/" + tagKey: clusters}}},
		)
	}
	return sts
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func toSet[V any](m map[string]V) map[string]bool {
	out := map[string]bool{}
	for k := range m {
		out[k] = true
	}
	return out
}

func safe(s string) string {
	b := []byte(s)
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			b[i] = '_'
		}
	}
	return string(b)
}

// QueryConfig is an environment's query-service grants, in queryd's
// `claims.group_grants` shape (auth.Grant with tuples).
func QueryConfig(out *Output, env string) map[string]any {
	gg := map[string]any{}
	for g, ts := range out.Query[env] {
		gg[g] = map[string]any{"tuples": ts}
	}
	return map[string]any{"environment": env, "group_grants": gg}
}

// Marshal is indented JSON with a trailing newline.
func Marshal(v any) []byte {
	b, _ := json.MarshalIndent(v, "", "  ")
	return append(b, '\n')
}
