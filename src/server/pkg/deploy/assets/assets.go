package assets

import (
	"fmt"
	"io"
	"path/filepath"
	"strconv"

	"github.com/pachyderm/pachyderm/src/server/pfs/server"
	"github.com/pachyderm/pachyderm/src/server/pkg/deploy"
	"github.com/ugorji/go/codec"
	"k8s.io/kubernetes/pkg/api/resource"
	"k8s.io/kubernetes/pkg/api/unversioned"
	api "k8s.io/kubernetes/pkg/api/v1"
	extensions "k8s.io/kubernetes/pkg/apis/extensions/v1beta1"
)

var (
	suite                  = "pachyderm"
	volumeSuite            = "pachyderm-pps-storage"
	pachdImageName         = "pachd"
	jobShimImageName       = "job-shim"
	pachydermRegistry      = "pachyderm"
	etcdImage              = "gcr.io/google_containers/etcd:2.0.12"
	rethinkImage           = "rethinkdb:2.3.3"
	serviceAccountName     = "pachyderm"
	etcdName               = "etcd"
	pachdName              = "pachd"
	rethinkName            = "rethink"
	rethinkVolumeName      = "rethink-volume"
	rethinkVolumeClaimName = "rethink-volume-claim"
	amazonSecretName       = "amazon-secret"
	googleSecretName       = "google-secret"
	microsoftSecretName    = "microsoft-secret"
	initName               = "pachd-init"
	trueVal                = true
)

type backend int

const (
	localBackend backend = iota
	amazonBackend
	googleBackend
	microsoftBackend
)

func (b backend) String() string {
	switch b {
	case localBackend:
		return "Local"
	case amazonBackend:
		return "Amazon"
	case googleBackend:
		return "Google"
	case microsoftBackend:
		return "Microsoft"
	default:
		return "Unknown backend " + strconv.Itoa(int(b))
	}
}

// K8s options for Google pach cluster
type GoogleManifestOptions struct {
	bucket     string
	volumeName string
	volumeSize int
	// version    string -- replaced by ManifestOptions.image
}

// K8s options for Amazon pach cluster
type AmazonManifestOptions struct {
	bucket     string
	volumeName string
	volumeSize int
	id         string
	secret     string
	token      string
	region     string
	// version    string -- replaced by ManifestOptions.image
}

// K8s options for Microsoft pach cluster
type MicrosoftManifestOptions struct {
	container string
	id        string
	secret    string
}

// K8s options for local pach cluster (no persistent storage or block store)
type LocalManifestOptions struct {
	hostPath string
}

type ManifestOptions struct {
	shards   uint64
	registry string
	version  string

	// The backend being used
	backend backend
	// Exactly one of the following must be non-nil
	localOptions     *LocalManifestOptions
	googleOptions    *GoogleManifestOptions
	amazonOptions    *AmazonManifestOptions
	microsoftOptions *MicrosoftManifestOptions
}

func (o *ManifestOptions) GetBucket() (string, error) {
	switch o.backend {
	case googleBackend:
		return o.googleOptions.bucket, nil
	case amazonBackend:
		return o.amazonOptions.bucket, nil
	default:
		return "", fmt.Errorf("Bucket not defined for backend \"" + o.backend.String() + "\"")
	}
}

func (o *ManifestOptions) GetVolumeName() (string, error) {
	switch o.backend {
	case googleBackend:
		return o.googleOptions.volumeName, nil
	case amazonBackend:
		return o.amazonOptions.volumeName, nil
	default:
		return "", fmt.Errorf("VolumeName not defined for backend \"" + o.backend.String() + "\"")
	}
}

func (o *ManifestOptions) GetVolumeSize() (int, error) {
	switch o.backend {
	case googleBackend:
		return o.googleOptions.volumeSize, nil
	case amazonBackend:
		return o.amazonOptions.volumeSize, nil
	default:
		return -1, fmt.Errorf("VolumeSize not defined for backend \"" + o.backend.String() + "\"")
	}
}

// ServiceAccount returns a kubernetes service account for use with Pachyderm.
func ServiceAccount() *api.ServiceAccount {
	return &api.ServiceAccount{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "ServiceAccount",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   serviceAccountName,
			Labels: labels(""),
		},
	}
}

// PachdRc returns a pachd replication controller.
// func PachdRc(shards uint64, backend backend, hostPath string, version string) *api.ReplicationController {
func PachdRc(opts ManifestOptions) *api.ReplicationController {
	tagSuffix := ""
	if opts.version != "" {
		tagSuffix = fmt.Sprintf(":%s", opts.version)
	}
	registry := pachydermRegistry
	if opts.registry != "" {
		registry = opts.registry
	}
	pachdImage := fmt.Sprintf("%s/%s%s", registry, pachdImageName, tagSuffix)
	jobShimImage := fmt.Sprintf("%s/%s%s", registry, jobShimImageName, tagSuffix)
	// we turn metrics on only if we have a static version, to prevent dev clusters from reporting metrics
	metrics := "true"
	if opts.version == deploy.DevVersionTag {
		metrics = "false"
	}
	volumes := []api.Volume{
		{
			Name: "pach-disk",
		},
	}
	volumeMounts := []api.VolumeMount{
		{
			Name:      "pach-disk",
			MountPath: "/pach",
		},
	}
	readinessProbe := &api.Probe{
		Handler: api.Handler{
			Exec: &api.ExecAction{
				Command: []string{
					"./pachd",
					"--readiness-check",
				},
			},
		},
		InitialDelaySeconds: 15,
		TimeoutSeconds:      1,
	}
	var backendEnvVar string
	switch opts.backend {
	case localBackend:
		volumes[0].HostPath = &api.HostPathVolumeSource{
			Path: filepath.Join(opts.localOptions.hostPath, "pachd"),
		}
	case amazonBackend:
		backendEnvVar = server.AmazonBackendEnvVar
		volumes = append(volumes, api.Volume{
			Name: amazonSecretName,
			VolumeSource: api.VolumeSource{
				Secret: &api.SecretVolumeSource{
					SecretName: amazonSecretName,
				},
			},
		})
		volumeMounts = append(volumeMounts, api.VolumeMount{
			Name:      amazonSecretName,
			MountPath: "/" + amazonSecretName,
		})
	case googleBackend:
		backendEnvVar = server.GoogleBackendEnvVar
		volumes = append(volumes, api.Volume{
			Name: googleSecretName,
			VolumeSource: api.VolumeSource{
				Secret: &api.SecretVolumeSource{
					SecretName: googleSecretName,
				},
			},
		})
		volumeMounts = append(volumeMounts, api.VolumeMount{
			Name:      googleSecretName,
			MountPath: "/" + googleSecretName,
		})
	case microsoftBackend:
		backendEnvVar = server.MicrosoftBackendEnvVar
		volumes = append(volumes, api.Volume{
			Name: microsoftSecretName,
			VolumeSource: api.VolumeSource{
				Secret: &api.SecretVolumeSource{
					SecretName: microsoftSecretName,
				},
			},
		})
		volumeMounts = append(volumeMounts, api.VolumeMount{
			Name:      microsoftSecretName,
			MountPath: "/" + microsoftSecretName,
		})
	}
	replicas := int32(1)
	return &api.ReplicationController{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   pachdName,
			Labels: labels(pachdName),
		},
		Spec: api.ReplicationControllerSpec{
			Replicas: &replicas,
			Selector: map[string]string{
				"app": pachdName,
			},
			Template: &api.PodTemplateSpec{
				ObjectMeta: api.ObjectMeta{
					Name:   pachdName,
					Labels: labels(pachdName),
				},
				Spec: api.PodSpec{
					Containers: []api.Container{
						{
							Name:  pachdName,
							Image: pachdImage,
							Env: []api.EnvVar{
								{
									Name:  "PACH_ROOT",
									Value: "/pach",
								},
								{
									Name:  "NUM_SHARDS",
									Value: strconv.FormatUint(opts.shards, 10),
								},
								{
									Name:  "STORAGE_BACKEND",
									Value: backendEnvVar,
								},
								{
									Name: "PACHD_POD_NAMESPACE",
									ValueFrom: &api.EnvVarSource{
										FieldRef: &api.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "metadata.namespace",
										},
									},
								},
								{
									Name:  "JOB_SHIM_IMAGE",
									Value: fmt.Sprintf(jobShimImage),
								},
								{
									Name:  "JOB_IMAGE_PULL_POLICY",
									Value: "IfNotPresent",
								},
								{
									Name:  "PACHD_VERSION",
									Value: opts.version,
								},
								{
									Name:  "METRICS",
									Value: metrics,
								},
							},
							Ports: []api.ContainerPort{
								{
									ContainerPort: 650,
									Protocol:      "TCP",
									Name:          "api-grpc-port",
								},
								{
									ContainerPort: 651,
									Name:          "trace-port",
								},
							},
							VolumeMounts: volumeMounts,
							SecurityContext: &api.SecurityContext{
								Privileged: &trueVal, // god is this dumb
							},
							ReadinessProbe:  readinessProbe,
							ImagePullPolicy: "IfNotPresent",
						},
					},
					ServiceAccountName: serviceAccountName,
					Volumes:            volumes,
				},
			},
		},
	}
}

// PachdService returns a pachd service.
func PachdService() *api.Service {
	return &api.Service{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   pachdName,
			Labels: labels(pachdName),
		},
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeNodePort,
			Selector: map[string]string{
				"app": pachdName,
			},
			Ports: []api.ServicePort{
				{
					Port:     650,
					Name:     "api-grpc-port",
					NodePort: 30650,
				},
				{
					Port:     651,
					Name:     "trace-port",
					NodePort: 30651,
				},
			},
		},
	}
}

// EtcdRc returns an etcd replication controller.
func EtcdRc(opts ManifestOptions) *api.ReplicationController {
	hostPath := ""
	if opts.backend == localBackend {
		hostPath = opts.localOptions.hostPath
	}
	replicas := int32(1)
	return &api.ReplicationController{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   etcdName,
			Labels: labels(etcdName),
		},
		Spec: api.ReplicationControllerSpec{
			Replicas: &replicas,
			Selector: map[string]string{
				"app": etcdName,
			},
			Template: &api.PodTemplateSpec{
				ObjectMeta: api.ObjectMeta{
					Name:   etcdName,
					Labels: labels(etcdName),
				},
				Spec: api.PodSpec{
					Containers: []api.Container{
						{
							Name:  etcdName,
							Image: etcdImage,
							//TODO figure out how to get a cluster of these to talk to each other
							Command: []string{
								"/usr/local/bin/etcd",
								"--bind-addr=0.0.0.0:2379",
								"--data-dir=/var/data/etcd",
							},
							Ports: []api.ContainerPort{
								{
									ContainerPort: 2379,
									Name:          "client-port",
								},
								{
									ContainerPort: 2380,
									Name:          "peer-port",
								},
							},
							VolumeMounts: []api.VolumeMount{
								{
									Name:      "etcd-storage",
									MountPath: "/var/data/etcd",
								},
							},
							ImagePullPolicy: "IfNotPresent",
						},
					},
					Volumes: []api.Volume{
						{
							Name: "etcd-storage",
							VolumeSource: api.VolumeSource{
								HostPath: &api.HostPathVolumeSource{
									Path: filepath.Join(hostPath, "etcd"),
								},
							},
						},
					},
				},
			},
		},
	}
}

// EtcdService returns an etcd service.
func EtcdService() *api.Service {
	return &api.Service{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   etcdName,
			Labels: labels(etcdName),
		},
		Spec: api.ServiceSpec{
			Selector: map[string]string{
				"app": etcdName,
			},
			Ports: []api.ServicePort{
				{
					Port: 2379,
					Name: "client-port",
				},
				{
					Port: 2380,
					Name: "peer-port",
				},
			},
		},
	}
}

// RethinkRc returns a rethinkdb replication controller.
func RethinkRc(opts ManifestOptions) *api.ReplicationController {
	replicas := int32(1)
	spec := &api.ReplicationController{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   rethinkName,
			Labels: labels(rethinkName),
		},
		Spec: api.ReplicationControllerSpec{
			Replicas: &replicas,
			Selector: map[string]string{
				"app": rethinkName,
			},
			Template: &api.PodTemplateSpec{
				ObjectMeta: api.ObjectMeta{
					Name:   rethinkName,
					Labels: labels(rethinkName),
				},
				Spec: api.PodSpec{
					Containers: []api.Container{
						{
							Name:  rethinkName,
							Image: rethinkImage,
							//TODO figure out how to get a cluster of these to talk to each other
							Command: []string{"rethinkdb", "-d", "/var/rethinkdb/data", "--bind", "all"},
							Ports: []api.ContainerPort{
								{
									ContainerPort: 8080,
									Name:          "admin-port",
								},
								{
									ContainerPort: 28015,
									Name:          "driver-port",
								},
								{
									ContainerPort: 29015,
									Name:          "cluster-port",
								},
							},
							VolumeMounts: []api.VolumeMount{
								{
									Name:      "rethink-storage",
									MountPath: "/var/rethinkdb/",
								},
							},
							ImagePullPolicy: "IfNotPresent",
						},
					},
					Volumes: []api.Volume{
						{
							Name: "rethink-storage",
						},
					},
				},
			},
		},
	}

	if opts.backend != localBackend {
		volumeName, err := opts.GetVolumeName()
		if err != nil {
			panic(fmt.Sprintf("Malformed Manifest options produced error %s", err.Error()))
		}
		if volumeName != "" {
			spec.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim = &api.PersistentVolumeClaimVolumeSource{
				ClaimName: rethinkVolumeClaimName,
			}
		}
	} else if opts.backend == localBackend || opts.backend == microsoftBackend {
		hostPath := ""
		if opts.backend == localBackend {
			hostPath = opts.localOptions.hostPath
		}
		// ToDo: workaround until https://github.com/pachyderm/pachyderm/issues/960
		spec.Spec.Template.Spec.Volumes[0].HostPath = &api.HostPathVolumeSource{
			Path: filepath.Join(hostPath, "rethink"),
		}
	}

	return spec
}

// RethinkService returns a rethinkdb service.
func RethinkService() *api.Service {
	return &api.Service{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   rethinkName,
			Labels: labels(rethinkName),
		},
		Spec: api.ServiceSpec{
			Type: api.ServiceTypeNodePort,
			Selector: map[string]string{
				"app": rethinkName,
			},
			Ports: []api.ServicePort{
				{
					Port:     8080,
					Name:     "admin-port",
					NodePort: 32080,
				},
				{
					Port:     28015,
					Name:     "driver-port",
					NodePort: 32081,
				},
				{
					Port:     29015,
					Name:     "cluster-port",
					NodePort: 32082,
				},
			},
		},
	}
}

// InitJob returns a pachd-init job.
func InitJob(opts ManifestOptions) *extensions.Job {
	pachdImage := pachdImageName
	if opts.version != "" {
		pachdImage += ":" + opts.version
	}
	registry := pachydermRegistry
	if opts.registry != "" {
		registry = opts.registry
	}
	pachdImage = fmt.Sprintf("%s/%s", registry, pachdImage)
	return &extensions.Job{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Job",
			APIVersion: "extensions/v1beta1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   initName,
			Labels: labels(initName),
		},
		Spec: extensions.JobSpec{
			Selector: &extensions.LabelSelector{
				MatchLabels: labels(initName),
			},
			Template: api.PodTemplateSpec{
				ObjectMeta: api.ObjectMeta{
					Name:   initName,
					Labels: labels(initName),
				},
				Spec: api.PodSpec{
					Containers: []api.Container{
						{
							Name:  initName,
							Image: pachdImage,
							Env: []api.EnvVar{
								{
									Name:  "PACH_ROOT",
									Value: "/pach",
								},
								{
									Name:  "INIT",
									Value: "true",
								},
							},
							ImagePullPolicy: "IfNotPresent",
						},
					},
					RestartPolicy: "OnFailure",
				},
			},
		},
	}
}

// AmazonSecret creates an amazon secret with the following parameters:
//   bucket - S3 bucket name
//   id     - AWS access key id
//   secret - AWS secret access key
//   token  - AWS access token
//   region - AWS region
func AmazonSecret(bucket string, id string, secret string, token string, region string) *api.Secret {
	return &api.Secret{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   amazonSecretName,
			Labels: labels(amazonSecretName),
		},
		Data: map[string][]byte{
			"bucket": []byte(bucket),
			"id":     []byte(id),
			"secret": []byte(secret),
			"token":  []byte(token),
			"region": []byte(region),
		},
	}
}

// GoogleSecret creates a google secret with a bucket name.
func GoogleSecret(bucket string) *api.Secret {
	return &api.Secret{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   googleSecretName,
			Labels: labels(googleSecretName),
		},
		Data: map[string][]byte{
			"bucket": []byte(bucket),
		},
	}
}

// MicrosoftSecret creates a microsoft secret with following parameters:
//   container - Azure blob container
//   id    	   - Azure storage account name
//   secret    - Azure storage account key
func MicrosoftSecret(container string, id string, secret string) *api.Secret {
	return &api.Secret{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   microsoftSecretName,
			Labels: labels(microsoftSecretName),
		},
		Data: map[string][]byte{
			"container": []byte(container),
			"id":        []byte(id),
			"secret":    []byte(secret),
		},
	}
}

// RethinkVolume creates a persistent volume with a backend
// (local, amazon, google), a name, and a size in gigabytes.
func RethinkVolume(backend backend, name string, size int) *api.PersistentVolume {
	spec := &api.PersistentVolume{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "PersistentVolume",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   rethinkVolumeName,
			Labels: volumeLabels(rethinkVolumeName),
		},
		Spec: api.PersistentVolumeSpec{
			Capacity: map[api.ResourceName]resource.Quantity{
				"storage": resource.MustParse(fmt.Sprintf("%vGi", size)),
			},
			AccessModes:                   []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: api.PersistentVolumeReclaimRetain,
		},
	}

	switch backend {
	case amazonBackend:
		spec.Spec.PersistentVolumeSource = api.PersistentVolumeSource{
			AWSElasticBlockStore: &api.AWSElasticBlockStoreVolumeSource{
				FSType:   "ext4",
				VolumeID: name,
			},
		}
	case googleBackend:
		spec.Spec.PersistentVolumeSource = api.PersistentVolumeSource{
			GCEPersistentDisk: &api.GCEPersistentDiskVolumeSource{
				FSType: "ext4",
				PDName: name,
			},
		}
	default:
		panic("cannot generate volume spec for unknown backend")
	}

	return spec
}

// RethinkVolumeClaim creates a persistent volume claim with a size in gigabytes.
func RethinkVolumeClaim(size int) *api.PersistentVolumeClaim {
	return &api.PersistentVolumeClaim{
		TypeMeta: unversioned.TypeMeta{
			Kind:       "PersistentVolumeClaim",
			APIVersion: "v1",
		},
		ObjectMeta: api.ObjectMeta{
			Name:   rethinkVolumeClaimName,
			Labels: volumeLabels(rethinkVolumeClaimName),
		},
		Spec: api.PersistentVolumeClaimSpec{
			Resources: api.ResourceRequirements{
				Requests: map[api.ResourceName]resource.Quantity{
					"storage": resource.MustParse(fmt.Sprintf("%vGi", size)),
				},
			},
			AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
		},
	}
}

// WriteAssets creates the assets in a dir. It expects dir to already exist.
/*** old signature:
/*** WriteAssets(writer, shards uint64, backend backend, volumeName string, volumeSize int, hostPath string, version string) {
*/
func WriteAssets(w io.Writer, opts ManifestOptions) error {
	encoder := codec.NewEncoder(w, &codec.JsonHandle{Indent: 2})

	ServiceAccount().CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")

	if opts.backend != localBackend {
		volumeName, err := opts.GetVolumeName()
		if err != nil {
			return err
		}
		volumeSize, err := opts.GetVolumeSize()
		if err != nil {
			return err
		}
		RethinkVolume(opts.backend, volumeName, volumeSize).CodecEncodeSelf(encoder)
		fmt.Fprintf(w, "\n")
		RethinkVolumeClaim(volumeSize).CodecEncodeSelf(encoder)
		fmt.Fprintf(w, "\n")
	}

	EtcdRc(opts).CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")
	EtcdService().CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")

	RethinkService().CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")
	RethinkRc(opts).CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")

	InitJob(opts).CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")

	PachdService().CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")
	PachdRc(opts).CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")
	return nil
}

// WriteLocalAssets writes assets to a local backend.
func WriteLocalAssets(w io.Writer, shards uint64, hostPath string, version string) {
	opts := ManifestOptions{
		shards:   shards,
		registry: "pachyderm",
		version:  "1.2.2",

		backend: localBackend,
		localOptions: &LocalManifestOptions{
			hostPath: hostPath,
		},
	}
	WriteAssets(w, opts)
}

// WriteAmazonAssets writes assets to an amazon backend.
func WriteAmazonAssets(w io.Writer, shards uint64, bucket string, id string, secret string, token string,
	region string, volumeName string, volumeSize int, version string) {
	opts := ManifestOptions{
		shards:   shards,
		registry: "pachyderm",
		version:  "1.2.2",

		backend: amazonBackend,
		amazonOptions: &AmazonManifestOptions{
			bucket:     bucket,
			volumeName: volumeName,
			volumeSize: volumeSize,
			id:         id,
			secret:     secret,
			token:      token,
			region:     region,
		},
	}
	WriteAssets(w, opts)
	encoder := codec.NewEncoder(w, &codec.JsonHandle{Indent: 2})
	AmazonSecret(bucket, id, secret, token, region).CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")
}

// WriteGoogleAssets writes assets to a google backend.
func WriteGoogleAssets(w io.Writer, shards uint64, bucket string,
	volumeName string, volumeSize int, version string) {
	opts := ManifestOptions{
		shards:   shards,
		registry: "pachyderm",
		version:  "1.2.2",

		backend: googleBackend,
		googleOptions: &GoogleManifestOptions{
			bucket:     bucket,
			volumeName: volumeName,
			volumeSize: volumeSize,
		},
	}
	WriteAssets(w, opts)
	encoder := codec.NewEncoder(w, &codec.JsonHandle{Indent: 2})
	GoogleSecret(bucket).CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")
}

// WriteMicrosoftAssets writes assets to a microsoft backend
func WriteMicrosoftAssets(w io.Writer, shards uint64, container string, id string, secret string,
	volumeURI string, volumeSize int, version string) {
	opts := ManifestOptions{
		shards:   shards,
		registry: "pachyderm",
		version:  "1.2.2",

		backend: microsoftBackend,
		microsoftOptions: &MicrosoftManifestOptions{
			container: container,
			id:        id,
			secret:    secret,
		},
	}
	WriteAssets(w, opts)
	encoder := codec.NewEncoder(w, &codec.JsonHandle{Indent: 2})
	MicrosoftSecret(container, id, secret).CodecEncodeSelf(encoder)
	fmt.Fprintf(w, "\n")
}

func labels(name string) map[string]string {
	return map[string]string{
		"app":   name,
		"suite": suite,
	}
}

func volumeLabels(name string) map[string]string {
	return map[string]string{
		"app":   name,
		"suite": volumeSuite,
	}
}
