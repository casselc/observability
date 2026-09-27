package ctrl

import (
	"strings"

	"github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
)

// Image splits a container image the way k8sattributes does
// (internal/common/docker.ParseImageName: reference.Parse, not normalized;
// tag "latest" when there is none).
func Image(image string) (name, tag string) {
	ref, err := reference.Parse(image)
	if err != nil {
		return "", ""
	}
	named, ok := ref.(reference.Named)
	if !ok {
		return "", ""
	}
	tag = "latest"
	if t, ok := named.(reference.Tagged); ok {
		tag = t.Tag()
	}
	return named.Name(), tag
}

// NodeAttrs is the node level: what k8sattributes (k8s.node.*) and
// resourcedetection (host.*, cloud.availability.zone) put on a resource.
func NodeAttrs(n *corev1.Node) map[string]string {
	m := map[string]string{
		"k8s.node.name": n.Name,
		"k8s.node.uid":  string(n.UID),
		"host.name":     n.Name,
	}
	if p := n.Spec.ProviderID; p != "" { // aws:///us-east-1a/i-0123
		m["host.id"] = p[strings.LastIndexByte(p, '/')+1:]
	}
	if v := n.Labels["node.kubernetes.io/instance-type"]; v != "" {
		m["host.type"] = v
	}
	if v := n.Labels["topology.kubernetes.io/zone"]; v != "" {
		m["cloud.availability.zone"] = v
	}
	return m
}

// StartTime is k8s.pod.start_time as k8sattributes writes it: the creation
// timestamp through metav1.Time.MarshalText (RFC 3339, UTC, seconds).
func StartTime(p *corev1.Pod) string {
	ts := p.GetCreationTimestamp()
	if ts.IsZero() {
		return ""
	}
	b, err := ts.MarshalText()
	if err != nil {
		return ""
	}
	return string(b)
}
