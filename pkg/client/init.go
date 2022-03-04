package client

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"text/template"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/client-go/discovery"
	memory "k8s.io/client-go/discovery/cached"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

// MCA, MCV represnts the corresponding resources
var (
	MCA    = "managedclusteractions"
	MCV    = "managedclusterviews"
	Failed = "FAILED"
	Done   = "DONE"
	NExist = "NON-EXISTENT"
	NErr   = "NO ERROR"
)

// Client provides a k8s dynamic client
type Client struct {
	Spoke            []string
	BackupPath       string
	KubeconfigPath   string
	KubernetesClient dynamic.Interface
}

// TemplateData provides template rendering data
type TemplateData struct {
	ResourceName string
	ClusterName  string
	RecoveryPath string
}

// ResourceTemplate define a resource template structure
type ResourceTemplate struct {
	// Must always correspond the Action or View resource name
	ResourceName string
	Template     string
}

// ActionCreateTemplates populates templates for creation of managedclusteraction resources
var ActionCreateTemplates = []ResourceTemplate{
	{"backup-create-namespace", mngClusterActCreateNS},
	{"backup-create-serviceaccount", mngClusterActCreateSA},
	{"backup-create-rolebinding", mngClusterActCreateRB},
	{"backup-create-job", mngClusterActCreateJob},
}

// ViewCreateTemplates populates templates for creation of managedclusterview resource
var ViewCreateTemplates = []ResourceTemplate{
	{"backup-create-clusterview", mngClusterViewJob},
}

// JobDeleteTemplates populates templates for creation of managedclusteraction resource to delete the namespace in the spoke
var JobDeleteTemplates = []ResourceTemplate{
	{"backup-delete-ns", mngClusterActDeleteNS},
}

// New creates a new instance of k8s client
// returns:			client, error
func New(Spoke []string, BackupPath string, KubeconfigPath string) (Client, error) {
	rand.Seed(time.Now().UnixNano())
	c := Client{Spoke, BackupPath, KubeconfigPath, nil}

	var clientset dynamic.Interface

	if KubeconfigPath != "" {
		// generate config from file
		config, err := c.GetConfig()
		if err != nil {
			log.Error(err)
			return c, err
		}
		// now try to connect to cluster
		clientset, err = dynamic.NewForConfig(config)
		if err != nil {
			log.Error(err)
			return c, err

		}
	} else {
		config, err := rest.InClusterConfig()
		if err != nil {
			log.Error(err)
			return c, err
		}

		// now try to connect to cluster
		clientset, err = dynamic.NewForConfig(config)
		if err != nil {
			log.Error(err)
			return c, err
		}
	}
	c.KubernetesClient = clientset

	return c, nil
}

// SpokeClusterExists verifies if a provided spoke cluster do exist or not
// returns:			bool
func (c Client) SpokeClusterExists(name string) bool {

	// using client, get if spoke cluster with given name exists
	gvr := schema.GroupVersionResource{
		Group:    "cluster.open-cluster-management.io",
		Version:  "v1",
		Resource: "managedclusters",
	}

	log.WithFields(log.Fields{"SpokeStatus": "Checking"}).Debugf("Checking if the Spoke cluster: %s exist...", name)
	foundSpokeCluster, err := c.KubernetesClient.Resource(gvr).Get(context.Background(), name, v1.GetOptions{})

	if err != nil {
		log.Error(err)
		return false
	}

	// transform to typed
	if foundSpokeCluster != nil {
		status, _, err := unstructured.NestedMap(foundSpokeCluster.Object, "status")
		if err != nil {
			log.Error(err)
			return false
		}
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
	log.WithFields(log.Fields{"SpokeStatus": "Found"}).Infof("Spoke cluster: %s exists", c.Spoke)
	return false
}

// GetConfig verifies providedkubeconfig
// returns:			*rest.Config, error
func (c Client) GetConfig() (*rest.Config, error) {
	config, err := clientcmd.BuildConfigFromFlags("", c.KubeconfigPath)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return config, nil
}

// LaunchKubernetesObjects creates managedclusteraction and managedclusterview resources from template
// returns:			error
func (c Client) LaunchKubernetesObjects(clusterName string, template []ResourceTemplate) error {

	config, err := c.GetConfig()
	if err != nil {
		log.Error(err)
		return err
	}

	newdata := TemplateData{
		ResourceName: "",
		ClusterName:  clusterName,
		RecoveryPath: c.BackupPath,
	}

	for _, item := range template {
		obj := &unstructured.Unstructured{}
		newdata.ResourceName = item.ResourceName

		log.Debug(strings.Repeat("-", 60))
		log.WithFields(log.Fields{"LaunchKubernetesObjects": "Launching"}).Debugf("Creating kubernetes object: [ %s ]", item.ResourceName)
		//	log.Debugf("####### Creating kubernetes object: [ %s ] #######", item.ResourceName)
		log.Debug(strings.Repeat("-", 60))

		log.Debugf("rendering resource: %s, data passed: %s for cluster: %s", item.ResourceName, newdata, clusterName)
		w, err := c.RenderYamlTemplate(item.ResourceName, item.Template, newdata)
		if err != nil && !errors.IsAlreadyExists(err) {
			return err
		}
		log.Debug("Retreiving GVK....")
		// decode YAML into unstructured.Unstructured
		dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
		_, gvk, err := dec.Decode(w.Bytes(), nil, obj)
		if err != nil {
			return err
		}

		log.Debugf("Retrieved GVK: %s", gvk)

		log.Debug("Mapping gvk to gvr with discovery client....")

		// Map GVK to GVR with discovery client
		discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
		if err != nil {
			return err
		}
		mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return err
		}

		log.Debug("Mapping has been successfully done")
		// Build resource
		resource := schema.GroupVersionResource{
			Group:    gvk.Group,
			Version:  gvk.Version,
			Resource: mapping.Resource.Resource,
		}
		log.WithFields(log.Fields{"LaunchKubernetesObjects": "Creating Resource"}).Debugf("CREATING the resource: [%s] at namespace: [backupresource] of spoke: [%s] ....", item.ResourceName, clusterName)
		//	log.Debugf("CREATING the resource: [%s] at namespace: [backupresource] of spoke: [%s] ....", item.ResourceName, clusterName)
		err = c.CreateKubernetesObjects(clusterName, obj, resource)
		if err != nil {
			log.Error(err)
			return err
		}

		log.Debug(strings.Repeat("-", 60))
		log.WithFields(log.Fields{"LaunchKubernetesObjects": "Created"}).Debugf("####### Successfully created the resource: [%s] at namespace: backupresource of spoke: [%s] ... #######", item.ResourceName, clusterName)
		//	log.Debugf("####### Successfully created the resource: [%s] at namespace: backupresource of spoke: [%s] ... #######", item.ResourceName, clusterName)
		log.Debug(strings.Repeat("-", 60))

	}
	return nil
}

// RenderYamlTemplate renders a single yaml template
//            resourceName - resource name
//            templateBody - template body
// returns:   bytes.Buffer rendered template
//            error
func (c Client) RenderYamlTemplate(resourceName string, templatebody string, data TemplateData) (*bytes.Buffer, error) {

	w := new(bytes.Buffer)

	//log.Debugf("Parsing template: %s", resourceName)
	log.WithFields(log.Fields{"Rendertemplate": "Starting"}).Debugf("Parsing template: %s", resourceName)

	tmpl, err := template.New(resourceName).Parse(commonTemplates + templatebody)
	if err != nil {
		return w, fmt.Errorf("failed to parse template %s: %v", resourceName, err)
	}
	data.ResourceName = resourceName
	err = tmpl.Execute(w, data)
	if err != nil {
		return w, fmt.Errorf("failed to render template %s: %v", resourceName, err)
	}
	//	log.Debugf("Successfully parsed template: %s", resourceName)
	log.WithFields(log.Fields{"Rendertemplate": "Done"}).Debugf("Successfully parsed template: %s", resourceName)
	return w, nil
}

// CreateKubernetesObjects creates specific mca and mcv object targeted to spoke cluster based on
// unstructured object and gvr
// returns:			error
func (c Client) CreateKubernetesObjects(clusterName string, obj *unstructured.Unstructured, resource schema.GroupVersionResource) error {

	_, err := c.KubernetesClient.Resource(resource).Namespace(clusterName).Create(context.Background(), obj, v1.CreateOptions{})
	if err != nil {
		log.Debugf("err is : %s", err)
		return err
	}
	return nil
}

// ManageObjects can query and delete k8s resource
// returns:			*unstructured.Unstructured (view data)
//                   error
func (c Client) ManageObjects(clusterName string, template []ResourceTemplate, resourceType string, action string) (*unstructured.Unstructured, error) {

	gvr := schema.GroupVersionResource{
		Group:    "view.open-cluster-management.io",
		Version:  "v1beta1",
		Resource: resourceType,
	}

	var view *unstructured.Unstructured

	for _, items := range template {
		switch action {
		case "get":
			view, err := c.KubernetesClient.Resource(gvr).Namespace(clusterName).Get(context.Background(), items.ResourceName, v1.GetOptions{})
			if err != nil {
				return view, err
			}
			return view, nil

		case "delete":
			err := c.KubernetesClient.Resource(gvr).Namespace(clusterName).Delete(context.Background(), items.ResourceName, v1.DeleteOptions{})
			if err != nil {
				return nil, err
			}
			log.WithFields(log.Fields{"DeleteObject": "Done"}).Debugf("####### Successfully deleted the %s resource named: [%s] for cluster: %s #######", resourceType, items.ResourceName, clusterName)
		//	log.Debugf("####### Successfully deleted the %s resource named: [%s] for cluster: %s #######", resourceType, items.ResourceName, clusterName)

		default:
			return nil, fmt.Errorf("no condition matched")
		}
	}
	return view, nil
}

// CheckViewProcessing checks whether managedclusterview is processing
// returns: 	processing bool
func (c Client) CheckViewProcessing(viewConditions []interface{}) string {
	// probably it is better to check if the result field is not empty and  status and type
	// need to verify
	var status, message string
	for _, condition := range viewConditions {
		status = condition.(map[string]interface{})["status"].(string)
		message = condition.(map[string]interface{})["message"].(string)
		log.Debugf("job status from mcv status: [%s], message: [%s]", status, message)
	}
	return status
}

// CheckStatus checks whether the job launched on the spoke was successfully launched and finished
// returns: 	error
func (c Client) CheckStatus(resourceType string, clusterName string) error {

	// Comment: this function must be improved to take into account that there should be a timeout window and
	// if the value returns false after the window, an error should be returned.

	// this is static for now, it should be parametrized.
	for i := 0; i < 10; i++ {

		time.Sleep(1 * time.Second)
		log.Debug("####### Checking if managedclusterview related to job is present #######")

		clusterView, err := c.ManageObjects(clusterName, ViewCreateTemplates, resourceType, "get")
		if err != nil {
			log.Errorf("Couldn't find managedclusterview from %s cluster; err: %s", c.Spoke, err)
			return err
		}
		log.Debug("Found managedclusterview object")

		conditions, exists, err := unstructured.NestedSlice(clusterView.Object, "status", "conditions")
		if err != nil {
			log.Error(err)
			return err
		}
		log.Debugf("conditions: %s", conditions)
		if !exists {
			return fmt.Errorf("couldn't find the intended structure")
		}
		value := c.CheckViewProcessing(conditions)
		log.Debugf("value is %s", value)
		if value == "True" {
			break
		}

	}
	log.Debug("####### out of the loop #######")
	return nil
}
