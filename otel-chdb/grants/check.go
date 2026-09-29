package grants

import (
	"fmt"
	"sort"
	"strings"

	cedar "github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"
)

// Check compares the compiled output with Cedar's own authorizer over the
// registry's whole finite universe: every environment, every registered
// cluster plus one unregistered cluster per environment, every known
// namespace plus one unnamed one per cluster; a probe principal per group
// and one holding every group (grants combine as a union); the read
// actions, and write for a workload of each cluster. accepted must be the
// policies Compile accepted. Any disagreement is a compiler bug, and the
// set is refused.
func Check(accepted cedar.PolicyList, out *Output, reg *Registry) error {
	ps := cedar.NewPolicySet()
	groups := map[string]bool{}
	for i, p := range accepted {
		ps.Add(cedar.PolicyID(fmt.Sprint(i)), p)
	}
	for _, gs := range out.Query {
		for g := range gs {
			groups[g] = true
		}
	}
	for _, gs := range out.TagWrites {
		for _, g := range gs {
			groups[g] = true
		}
	}
	for _, gs := range out.LiteralWrites {
		for g := range gs {
			groups[g] = true
		}
	}
	ents := types.EntityMap{}
	put := func(typ, id string, parents ...types.EntityUID) types.EntityUID {
		u := cedar.NewEntityUID(types.EntityType(typ), types.String(id))
		ents[u] = types.Entity{UID: u, Parents: cedar.NewEntityUIDSet(parents...), Attributes: cedar.NewRecord(nil)}
		return u
	}
	type nsRef struct {
		env, cluster, ns string
		uid              types.EntityUID
	}
	var universe []nsRef
	for _, e := range reg.envNames() {
		eu := put(TEnv, e)
		cls := append(reg.Envs[e].clusterNames(), unlistedCl+"-"+e)
		for _, c := range cls {
			var cu types.EntityUID
			if _, ok := reg.Envs[e].Clusters[c]; ok {
				cu = put(TCluster, c, eu)
			} else {
				cu = put(TCluster, c) // in no environment: the registry does not hold it
			}
			nss := append(append([]string(nil), reg.Envs[e].Clusters[c]...), unnamedNs)
			for _, n := range nss {
				universe = append(universe, nsRef{e, c, n, put(TNamespace, c+"/"+n, cu)})
			}
		}
	}
	gl := keys(groups)
	var probes [][]string
	for _, g := range gl {
		probes = append(probes, []string{g})
	}
	if len(gl) > 1 {
		probes = append(probes, gl)
	}
	var errs []string
	// structure: a tuple names a cluster of its own environment
	for e, gs := range out.Query {
		for g, ts := range gs {
			for _, t := range ts {
				if env, ok := reg.envOf(t.Cluster); !ok || env != e {
					errs = append(errs, fmt.Sprintf("%s: group %s's tuple %v names a cluster of environment %q", e, g, t, env))
				}
			}
		}
	}
	for pi, held := range probes {
		var parents []types.EntityUID
		for _, g := range held {
			parents = append(parents, put(TGroup, g))
		}
		user := put(TUser, fmt.Sprintf("%s%d", probePrefix, pi), parents...)
		// a workload holding the same groups reads the same (queryd cannot
		// tell them apart)
		wl := cedar.NewEntityUID(TWorkload, types.String(fmt.Sprintf("%sr%d", probePrefix, pi)))
		ents[wl] = types.Entity{UID: wl, Parents: cedar.NewEntityUIDSet(parents...),
			Attributes: cedar.NewRecord(cedar.RecordMap{"cluster": cedar.NewEntityUID(TCluster, types.String(universe[0].cluster))})}
		for _, who := range []types.EntityUID{user, wl} {
			for _, r := range universe {
				for _, act := range ReadActions {
					want := allowsRead(out, held, act, r.env, r.cluster, r.ns)
					got, _ := cedar.Authorize(ps, ents, cedar.Request{Principal: who, Action: cedar.NewEntityUID(TAction, types.String(act)), Resource: r.uid, Context: cedar.NewRecord(nil)})
					if bool(got) != want {
						errs = append(errs, fmt.Sprintf("%v (%s) %s %s/%s in %s: Cedar %v, compiled %v", held, who.Type, act, r.cluster, r.ns, r.env, got, want))
					}
				}
			}
		}
		delete(ents, wl)
		// write: a workload of each cluster (registered or not), on every
		// namespace of the universe
		for _, own := range universe {
			if own.ns != unnamedNs {
				continue
			}
			wid := fmt.Sprintf("%sw%d-%s", probePrefix, pi, own.cluster)
			wu := cedar.NewEntityUID(TWorkload, types.String(wid))
			ents[wu] = types.Entity{UID: wu, Parents: cedar.NewEntityUIDSet(parents...),
				Attributes: cedar.NewRecord(cedar.RecordMap{"cluster": cedar.NewEntityUID(TCluster, types.String(own.cluster))})}
			for _, r := range universe {
				want := allowsWrite(out, held, own.cluster, r.env, r.cluster, reg)
				got, _ := cedar.Authorize(ps, ents, cedar.Request{Principal: wu, Action: cedar.NewEntityUID(TAction, ActWrite), Resource: r.uid, Context: cedar.NewRecord(nil)})
				if bool(got) != want {
					errs = append(errs, fmt.Sprintf("%v write by a workload of %s on %s/%s in %s: Cedar %v, compiled %v", held, own.cluster, r.cluster, r.ns, r.env, got, want))
				}
			}
			delete(ents, wu)
		}
		delete(ents, user)
	}
	if len(errs) > 0 {
		sort.Strings(errs)
		if len(errs) > 20 {
			errs = append(errs[:20], fmt.Sprintf("… %d more", len(errs)-20))
		}
		return fmt.Errorf("compiled grants disagree with Cedar:\n  %s", strings.Join(errs, "\n  "))
	}
	return nil
}

// allowsRead: some held group's tuple in env covers (act, cluster, ns).
func allowsRead(out *Output, held []string, act, env, cluster, ns string) bool {
	for _, g := range held {
		for _, t := range out.Query[env][g] {
			if t.Role == act && (t.Cluster == "*" || t.Cluster == cluster) && (t.Namespace == "*" || t.Namespace == ns) {
				return true
			}
		}
	}
	return false
}

// allowsWrite is what the compiled IAM allows: a tag-bound write in env
// for a workload whose cluster is the resource's and registered in env
// (the Deny on other clusters), or a literal write of the cluster.
func allowsWrite(out *Output, held []string, own, env, cluster string, reg *Registry) bool {
	for _, g := range held {
		for _, tg := range out.TagWrites[env] {
			if tg == g && own == cluster {
				if _, ok := reg.Envs[env].Clusters[own]; ok {
					return true
				}
			}
		}
		for _, c := range out.LiteralWrites[env][g] {
			if c == cluster {
				return true
			}
		}
	}
	return false
}
