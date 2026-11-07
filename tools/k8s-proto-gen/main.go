// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2026 Sascha Brawer

// Command k8s-proto-gen refreshes everything kube-shim needs to accept
// Kubernetes protobuf request bodies (issue #106), from the k8s.io/api
// and k8s.io/apimachinery versions pinned in this directory's go.mod:
//
//   - proto/k8s.io/...: the generated.proto files, which build.rs
//     compiles with protox.
//   - proto/go-fields.json: for each protobuf message kube-shim may
//     decode, how Go's encoding/json writes each field (JSON name,
//     omitempty, pointer, inline). Protobuf alone can't say whether a
//     zero value was set or not, so kube-shim needs this to produce the
//     same JSON the Go types would.
//   - tests/testdata/protobuf/*.pb and *.json: request bodies encoded
//     the way client-go encodes them, and the JSON that Go produces
//     after decoding that same protobuf. kube-shim's tests check that
//     converting each .pb gives exactly the .json.
//
// Run it from the repository root with:
//
//	(cd tools/k8s-proto-gen && go run .)
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
)

var repo = flag.String("repo", "../..", "kube-shim repository root")

// The proto files kube-shim compiles, by Go module.
var protoFiles = map[string][]string{
	"k8s.io/api": {
		"core/v1/generated.proto",
		"batch/v1/generated.proto",
	},
	"k8s.io/apimachinery": {
		"pkg/apis/meta/v1/generated.proto",
		"pkg/api/resource/generated.proto",
		"pkg/util/intstr/generated.proto",
		"pkg/runtime/generated.proto",
		"pkg/runtime/schema/generated.proto",
	},
}

// Types with their own JSON encoding, which kube-shim's converter
// handles by message name rather than field by field.
var specialTypes = map[reflect.Type]bool{
	reflect.TypeOf(metav1.Time{}):          true,
	reflect.TypeOf(metav1.MicroTime{}):     true,
	reflect.TypeOf(metav1.Duration{}):      true,
	reflect.TypeOf(metav1.FieldsV1{}):      true,
	reflect.TypeOf(resource.Quantity{}):    true,
	reflect.TypeOf(intstr.IntOrString{}):   true,
	reflect.TypeOf(runtime.RawExtension{}): true,
}

func main() {
	flag.Parse()
	copyProtos()
	writeFieldTable()
	writeFixtures()
}

func copyProtos() {
	for module, files := range protoFiles {
		out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", module).Output()
		if err != nil {
			log.Fatalf("go list %s: %v", module, err)
		}
		dir := strings.TrimSpace(string(out))
		for _, f := range files {
			data, err := os.ReadFile(filepath.Join(dir, f))
			if err != nil {
				log.Fatal(err)
			}
			writeFile(filepath.Join("proto", module, f), data)
		}
	}
}

// Field is one entry of proto/go-fields.json.
type Field struct {
	JSON      string `json:"json"`
	OmitEmpty bool   `json:"omitempty,omitempty"`
	Pointer   bool   `json:"pointer,omitempty"`
	Inline    bool   `json:"inline,omitempty"`
}

func writeFieldTable() {
	table := map[string]map[string]Field{}
	for _, root := range []any{corev1.Secret{}, batchv1.CronJob{}, batchv1.Job{}} {
		walk(reflect.TypeOf(root), table)
	}
	data, err := json.MarshalIndent(table, "", " ")
	if err != nil {
		log.Fatal(err)
	}
	writeFile("proto/go-fields.json", append(data, '\n'))
}

// protoMessageName is the full protobuf name of a Go API type, e.g.
// k8s.io.api.core.v1.Container for corev1.Container.
func protoMessageName(t reflect.Type) string {
	return strings.ReplaceAll(t.PkgPath(), "/", ".") + "." + t.Name()
}

func walk(t reflect.Type, table map[string]map[string]Field) {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Map {
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return // []byte
		}
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || specialTypes[t] {
		return
	}
	name := protoMessageName(t)
	if _, done := table[name]; done {
		return
	}
	fields := map[string]Field{}
	table[name] = fields
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		jsonTag := f.Tag.Get("json")
		if f.Type == reflect.TypeOf(metav1.TypeMeta{}) || jsonTag == "-" {
			continue // TypeMeta travels in runtime.Unknown, not in the message
		}
		protoTag := f.Tag.Get("protobuf")
		if protoTag == "" {
			log.Fatalf("%s.%s has no protobuf tag", name, f.Name)
		}
		parts := strings.Split(protoTag, ",")
		number, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Fatalf("%s.%s: bad protobuf tag %q", name, f.Name, protoTag)
		}
		jsonParts := strings.Split(jsonTag, ",")
		field := Field{
			JSON:    jsonParts[0],
			Pointer: f.Type.Kind() == reflect.Ptr,
		}
		for _, opt := range jsonParts[1:] {
			switch opt {
			case "omitempty":
				field.OmitEmpty = true
			case "inline":
				field.Inline = true
			}
		}
		if field.Inline != (field.JSON == "") {
			log.Fatalf("%s.%s: unexpected json tag %q", name, f.Name, jsonTag)
		}
		fields[strconv.Itoa(number)] = field
		walk(f.Type, table)
	}
}

type fixture struct {
	name string
	obj  runtime.Object
	gv   schema.GroupVersion
}

func writeFixtures() {
	codecs := scheme.Codecs
	pb := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)
	for _, fx := range fixtures() {
		// What client-go sends: the protobuf serializer, wrapped in the
		// same versioning encoder its REST client uses.
		body, err := runtime.Encode(codecs.EncoderForVersion(pb, fx.gv), fx.obj)
		if err != nil {
			log.Fatalf("%s: %v", fx.name, err)
		}
		writeFile(filepath.Join("tests/testdata/protobuf", fx.name+".pb"), body)

		// What Go makes of those same bytes, written as JSON.
		decoded, _, err := pb.Decode(body, nil, nil)
		if err != nil {
			log.Fatalf("%s: decode: %v", fx.name, err)
		}
		gvk := fx.gv.WithKind(reflect.TypeOf(fx.obj).Elem().Name())
		decoded.GetObjectKind().SetGroupVersionKind(gvk)
		js, err := json.MarshalIndent(decoded, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		writeFile(filepath.Join("tests/testdata/protobuf", fx.name+".json"), append(js, '\n'))
	}
}

func writeFile(rel string, data []byte) {
	path := filepath.Join(*repo, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Println("wrote", rel)
}

// osmdiffsPodSpec mirrors brawer/production's kube-shim-jobs/osmdiffs.tf,
// with the values the hashicorp/kubernetes provider fills in itself
// (privileged=false, mountPropagation=None, and so on).
func osmdiffsPodSpec() corev1.PodSpec {
	return corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		TerminationGracePeriodSeconds: ptr.To[int64](30),
		DNSPolicy:                     corev1.DNSClusterFirst,
		SecurityContext: &corev1.PodSecurityContext{
			FSGroup: ptr.To[int64](1000),
		},
		Containers: []corev1.Container{{
			Name:  "osmdiffs",
			Image: "ghcr.io/brawer/osmdiffs:latest",
			Args:  []string{"run", "--workdir", "/scratch", "--run_id", "$(RUN_ID)"},
			Env: []corev1.EnvVar{{
				Name: "RUN_ID",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			}},
			EnvFrom: []corev1.EnvFromSource{{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "osmdiffs-s3-credentials"},
				},
			}},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("6"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("6"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
			SecurityContext: &corev1.SecurityContext{
				Privileged:             ptr.To(false),
				ReadOnlyRootFilesystem: ptr.To(true),
			},
			VolumeMounts: []corev1.VolumeMount{{
				Name:             "scratch",
				MountPath:        "/scratch",
				MountPropagation: ptr.To(corev1.MountPropagationNone),
			}},
			TerminationMessagePath:   "/dev/termination-log",
			TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		}},
		Volumes: []corev1.Volume{{
			Name: "scratch",
			VolumeSource: corev1.VolumeSource{
				Ephemeral: &corev1.EphemeralVolumeSource{
					VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
						Spec: corev1.PersistentVolumeClaimSpec{
							AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
							StorageClassName: ptr.To("kube-shim-fast"),
							Resources: corev1.VolumeResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceStorage: resource.MustParse("250Gi"),
								},
							},
						},
					},
				},
			},
		}},
	}
}

func fixtures() []fixture {
	jobSpec := batchv1.JobSpec{
		BackoffLimit:          ptr.To[int32](1),
		ActiveDeadlineSeconds: ptr.To[int64](16 * 3600),
		Template:              corev1.PodTemplateSpec{Spec: osmdiffsPodSpec()},
	}

	// Fields kube-shim's admission doesn't allow, to cover the
	// converter's special types: IntOrString (int and string), a
	// pointer Quantity, labels and a non-zero Time.
	misc := osmdiffsPodSpec()
	misc.Containers[0].Ports = []corev1.ContainerPort{{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP}}
	misc.Containers[0].LivenessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")},
		},
		InitialDelaySeconds: 5,
	}
	misc.Containers[0].ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(8080)},
		},
	}
	misc.Volumes = append(misc.Volumes, corev1.Volume{
		Name: "tmp",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr.To(resource.MustParse("1500m"))},
		},
	})
	misc.Tolerations = []corev1.Toleration{{Key: "k", Operator: corev1.TolerationOpExists, TolerationSeconds: ptr.To[int64](0)}}

	return []fixture{
		{
			name: "secret",
			gv:   corev1.SchemeGroupVersion,
			obj: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "osmdiffs-s3-credentials"},
				Data: map[string][]byte{
					"PUBLIC_S3_BUCKET":            []byte("osmdiffs-data-de"),
					"PUBLIC_S3_ACCESS_KEY_SECRET": []byte("not-a-real-secret\x00\xff"),
				},
				Type: corev1.SecretTypeOpaque,
			},
		},
		{
			name: "cronjob",
			gv:   batchv1.SchemeGroupVersion,
			obj: &batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{Name: "osmdiffs-weekly", Namespace: "default"},
				Spec: batchv1.CronJobSpec{
					Schedule:                   "0 3 * * 0",
					Suspend:                    ptr.To(false),
					ConcurrencyPolicy:          batchv1.ForbidConcurrent,
					StartingDeadlineSeconds:    ptr.To[int64](3600),
					SuccessfulJobsHistoryLimit: ptr.To[int32](3),
					FailedJobsHistoryLimit:     ptr.To[int32](3),
					JobTemplate:                batchv1.JobTemplateSpec{Spec: jobSpec},
				},
			},
		},
		{
			name: "job",
			gv:   batchv1.SchemeGroupVersion,
			obj: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "osmdiffs-manual", Namespace: "default"},
				Spec:       jobSpec,
			},
		},
		{
			// Admission rejects this one: no activeDeadlineSeconds.
			name: "job-no-deadline",
			gv:   batchv1.SchemeGroupVersion,
			obj: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "no-deadline", Namespace: "default"},
				Spec:       batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: osmdiffsPodSpec()}},
			},
		},
		{
			name: "job-misc",
			gv:   batchv1.SchemeGroupVersion,
			obj: &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "misc",
					Labels:            map[string]string{"app": "misc"},
					Annotations:       map[string]string{"note": ""},
					CreationTimestamp: metav1.Date(2026, 11, 7, 1, 2, 3, 0, time.UTC),
				},
				Spec: batchv1.JobSpec{
					ActiveDeadlineSeconds: ptr.To[int64](60),
					Template:              corev1.PodTemplateSpec{Spec: misc},
				},
			},
		},
	}
}
