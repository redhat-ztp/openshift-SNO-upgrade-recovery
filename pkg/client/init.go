package client

import (
	"bytes"
	"context"
	"fmt"

	"text/template"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

var LIVE_POLICY string = "policy-backup-live-image"
var RELEASE_POLICY string = "policy-backup-release-image"

type Client struct {
	KubeconfigPath   string
	Spoke            string
	BinaryImage      string
	BackupPath       string
	KubernetesClient dynamic.Interface
}

type BackupImageSpoke struct {
	SpokeName             string
	PolicyName            string
	ImageBinaryImageName  string
	ImageURL              string
	RecoveryPartitionPath string
}

type PlacementBinding struct {
	PlacementName string
	PolicyName    string
}

func New(KubeconfigPath string, Spoke string, BinaryImage string, BackupPath string) (Client, error) {
	c := Client{KubeconfigPath, Spoke, BinaryImage, BackupPath, nil}

	// establish kubernetes connection
	config, err := clientcmd.BuildConfigFromFlags("", KubeconfigPath)
	if err != nil {
		log.Error(err)
		return c, err
	}

	// now try to connect to cluster
	clientset, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Error(err)
		return c, err
	}
	c.KubernetesClient = clientset

	return c, nil
}

func (c Client) SpokeClusterExists() bool {
	// using client, get if spoke cluster with given name exists
	gvr := schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "managedclusters",
	}

	foundSpokeCluster, err := c.KubernetesClient.Resource(gvr).Get(context.Background(), c.Spoke, v1.GetOptions{})

	if err != nil {
		log.Error(err)
		return false
	}

	// transform to typed
	if foundSpokeCluster != nil {
		status, _, _ := unstructured.NestedMap(foundSpokeCluster.Object, "status")
		if status != nil {
			if conditions, ok := status["conditions"]; ok {
				// check for condition
				for _, v := range conditions.([]interface{}) {
					key := v.(map[string]interface{})["type"]
					if key == "ManagedClusterConditionAvailable" {
						val := v.(map[string]interface{})["status"]
						if val == "True" {
							// exists and is available
							return true
						}
					}
				}
			}
		}

	}
	return false
}

// given a version, retrieves the matching rootfs
func (c Client) GetRootFsFromVersion(version string) (string, error) {
	gvr := schema.GroupVersionResource{
		Group:    "agent-install.openshift.io",
		Version:  "v1beta1",
		Resource: "agentserviceconfigs",
	}

	foundConfig, err := c.KubernetesClient.Resource(gvr).Get(context.Background(), "agent", v1.GetOptions{})

	if err != nil {
		log.Error(err)
		return "", err
	}

	if foundConfig != nil {
		// retrieve the images section
		spec, _, _ := unstructured.NestedMap(foundConfig.Object, "spec")
		images := spec["osImages"]

		// iterate over images until we find the matching version
		for _, v := range images.([]interface{}) {
			key := v.(map[string]interface{})["openshiftVersion"]
			if key == version {
				val := v.(map[string]interface{})["rootFSUrl"].(string)
				return val, nil
			}
		}

	}
	return "", err
}

// function to retrieve the openshift version and retrieve rootfs
func (c Client) GetRootFSUrl() (string, error) {
	// retrieve clusterdeployment for that spoke
	gvr := schema.GroupVersionResource{
		Group:    "hive.openshift.io",
		Version:  "v1",
		Resource: "clusterdeployments",
	}

	foundSpokeCluster, err := c.KubernetesClient.Resource(gvr).Namespace(c.Spoke).Get(context.Background(), c.Spoke, v1.GetOptions{})

	if err != nil {
		log.Error(err)
		return "", err
	}

	// transform to typed and retrieve version
	version := ""
	if foundSpokeCluster != nil {
		metadata, _, _ := unstructured.NestedMap(foundSpokeCluster.Object, "metadata")
		labels := metadata["labels"].(map[string]interface{})

		// check the label that starts with matching pattern
		for k, v := range labels {
			if k == "hive.openshift.io/version-major-minor" {
				// we have the version
				version = v.(string)
				break
			}
		}

		if version != "" {
			// we have version, let's extract rootfs
			rootfs, err := c.GetRootFsFromVersion(version)

			if err != nil {
				return "", err
			}

			return rootfs, nil
		}

	}
	return "", nil

}

// function to query an imageset and return the image
func (c Client) GetImageFromImageSet(name string) (string, error) {
	gvr := schema.GroupVersionResource{
		Group:    "hive.openshift.io",
		Version:  "v1",
		Resource: "clusterimagesets",
	}

	foundImageset, err := c.KubernetesClient.Resource(gvr).Get(context.Background(), name, v1.GetOptions{})

	if err != nil {
		log.Error(err)
		return "", err
	}

	if foundImageset != nil {
		// retrieve the images section
		spec, _, _ := unstructured.NestedMap(foundImageset.Object, "spec")
		release := spec["releaseImage"]

		if release != nil {
			return release.(string), nil
		}

	}
	return "", err
}

// function to retrieve a Release Image of a given cluster
func (c Client) GetReleaseImage() (string, error) {
	// retrieve agentclusterinstall for that spoke
	gvr := schema.GroupVersionResource{
		Group:    "extensions.hive.openshift.io",
		Version:  "v1beta1",
		Resource: "agentclusterinstalls",
	}

	foundSpokeCluster, err := c.KubernetesClient.Resource(gvr).Namespace(c.Spoke).Get(context.Background(), c.Spoke, v1.GetOptions{})

	if err != nil {
		log.Error(err)
		return "", err
	}

	// transform to typed and retrieve version
	if foundSpokeCluster != nil {
		spec, _, _ := unstructured.NestedMap(foundSpokeCluster.Object, "spec")
		imageSetRef := spec["imageSetRef"].(map[string]interface{})

		if imageSetRef != nil {
			clusterImageSetName := imageSetRef["name"].(string)
			if clusterImageSetName != "" {
				// need to retrieve url from imageset
				releaseImage, err := c.GetImageFromImageSet(clusterImageSetName)

				if err != nil {
					log.Error(err)
					return "", err
				}

				return releaseImage, nil
			}
		}
	}

	return "", nil
}

// create a generic placement binding
func (c Client) CreatePlacementBinding(PlacementBindingName string, PlacementRuleName string) error {
	var backupPolicy bytes.Buffer
	tmpl := template.New("policyBackupPlacementBindingTemplate")
	tmpl.Parse(policyBackupPlacementBindingTemplate)

	// create a new object for live image
	b := PlacementBinding{PlacementBindingName, PlacementRuleName}
	if err := tmpl.Execute(&backupPolicy, b); err != nil {
		log.Error(err)
		return err
	}

	// convert to unstructured
	finalPolicy := &unstructured.Unstructured{}
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	_, _, err := dec.Decode(backupPolicy.Bytes(), nil, finalPolicy)
	if err != nil {
		log.Error(err)
		return err
	}

	// once we have the policy, apply it
	gvr := schema.GroupVersionResource{
		Group:    "policy.open-cluster-management.io",
		Version:  "v1",
		Resource: "placementbindings",
	}

	_, err = c.KubernetesClient.Resource(gvr).Namespace("open-cluster-management").Create(context.Background(), finalPolicy, v1.CreateOptions{})
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// launch the backup for the spoke cluster, for the specific image
func (c Client) LaunchLiveImageBackup(liveImg string) error {
	// create placement binding in case it does not exist
	c.CreatePlacementBinding("placement-binding-backup-live-image", LIVE_POLICY)

	var backupPolicy bytes.Buffer
	tmpl := template.New("policyBackupLiveImageTemplate")
	tmpl.Parse(policyBackupLiveImageTemplate)

	// create a new object for live image
	b := BackupImageSpoke{c.Spoke, LIVE_POLICY, c.BinaryImage, liveImg, c.BackupPath}
	if err := tmpl.Execute(&backupPolicy, b); err != nil {
		log.Error(err)
		return err
	}

	// convert to unstructured
	finalPolicy := &unstructured.Unstructured{}
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	_, _, err := dec.Decode(backupPolicy.Bytes(), nil, finalPolicy)
	if err != nil {
		log.Error(err)
		return err
	}

	// once we have the policy, apply it
	gvr := schema.GroupVersionResource{
		Group:    "policy.open-cluster-management.io",
		Version:  "v1",
		Resource: "policies",
	}

	_, err = c.KubernetesClient.Resource(gvr).Namespace("open-cluster-management").Create(context.Background(), finalPolicy, v1.CreateOptions{})
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}

// creates a global placement rule for spoke
func (c Client) CreatePlacementRule() error {
	var backupPolicy bytes.Buffer
	tmpl := template.New("policySpokePlacementRuleTemplate")
	tmpl.Parse(policySpokePlacementRuleTemplate)

	// create a new object for spoke rule
	b := BackupImageSpoke{c.Spoke, "", "", "", ""}
	if err := tmpl.Execute(&backupPolicy, b); err != nil {
		log.Error(err)
		return err
	}

	// convert to unstructured
	finalPolicy := &unstructured.Unstructured{}
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	_, _, err := dec.Decode(backupPolicy.Bytes(), nil, finalPolicy)
	if err != nil {
		log.Error(err)
		return err
	}

	// once we have the rule, apply it
	gvr := schema.GroupVersionResource{
		Group:    "apps.open-cluster-management.io",
		Version:  "v1",
		Resource: "placementrules",
	}

	_, err = c.KubernetesClient.Resource(gvr).Namespace("open-cluster-management").Create(context.Background(), finalPolicy, v1.CreateOptions{})
	if err != nil {
		log.Error(err)
		return err
	}

	return nil

}

// removes all previously created resources
func (c Client) RemovePreviousResources() error {
	PoliciesList := []string{LIVE_POLICY, RELEASE_POLICY}

	for _, policy := range PoliciesList {
		// check if policy exists
		gvr := schema.GroupVersionResource{
			Group:    "policy.open-cluster-management.io",
			Version:  "v1",
			Resource: "policies",
		}

		resource, _ := c.KubernetesClient.Resource(gvr).Namespace("open-cluster-management").Get(context.Background(), policy, v1.GetOptions{})

		if resource != nil {
			// got it, remove it
			log.Info(fmt.Sprintf("Policy %s still exists, removing it", policy))
			err := c.KubernetesClient.Resource(gvr).Namespace("open-cluster-management").Delete(context.Background(), policy, v1.DeleteOptions{})
			if err != nil {
				return err
			}

		}

	}
	return nil

}

// launch the backup for the spoke cluster, for the specific release image
func (c Client) LaunchReleaseImageBackup(releaseImg string) error {
	// create placement binding in case it does not exist
	c.CreatePlacementBinding("placement-binding-backup-release-image", RELEASE_POLICY)

	var backupPolicy bytes.Buffer
	tmpl := template.New("policyBackupReleaseImageTemplate")
	tmpl.Parse(policyBackupReleaseImageTemplate)

	// create a new object for live image
	b := BackupImageSpoke{c.Spoke, RELEASE_POLICY, c.BinaryImage, releaseImg, c.BackupPath}
	if err := tmpl.Execute(&backupPolicy, b); err != nil {
		log.Error(err)
		return err
	}

	// convert to unstructured
	finalPolicy := &unstructured.Unstructured{}
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	_, _, err := dec.Decode(backupPolicy.Bytes(), nil, finalPolicy)
	if err != nil {
		log.Error(err)
		return err
	}

	// once we have the policy, apply it
	gvr := schema.GroupVersionResource{
		Group:    "policy.open-cluster-management.io",
		Version:  "v1",
		Resource: "policies",
	}

	_, err = c.KubernetesClient.Resource(gvr).Namespace("open-cluster-management").Create(context.Background(), finalPolicy, v1.CreateOptions{})
	if err != nil {
		log.Error(err)
		return err
	}

	return nil
}
