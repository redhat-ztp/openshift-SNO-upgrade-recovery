# openshift-ai-trigger-backup

Cli to orchestrate backup requests on an ACM hub to spoke clusters. It will trigger the backup jobs 
from <https://github.com/redhat-ztp/openshift-ai-image-backup>

## How to build

A Makefile is provided in order to facilitate the building of the client. Just execute `Make build` and 
it will place the generate binary into `bin/backup` relative path.

### Running from command line

In order to run trigger backup from command line, you need to have a valid KUBECONFIG file with 
valid credentials to access the hub cluster. You can then launch the command, that will trigger the backup 
on the desired spoke cluster:
`./bin/backup trigger-backup -k /tmp/kubeconfig_karmalabs -s 'spoke-cluster1, spoke-cluster-2'`

This command will create four managedclusterAction and one managedclusterView per spoke in the hub cluster,
that will launch the backup jobs in the spoke.
Once the job is finished, it will automatically remove managedclusterView on the hub and the created namaspace
in the spoke to clean up artifacts.

### Running from a job

In order to run as a job one can launch the job by following pkg/client/templmates.go file, where the 
launched objects template is provided.
