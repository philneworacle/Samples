import oci
import os

# This script downloads all of the usage reports for a tenancy (specified in the config file)
#
# Pre-requisites: Create an IAM policy to endorse users in your tenancy to read usage reports from the OCI tenancy
#
# Example policy:
# define tenancy usage-report as ocid1.tenancy.oc1..aaaaaaaaned4fkpkisbwjlr56u7cj63lf3wffbilvqknstgtvzub7vhqkggq
# endorse group <group_name> to read objects in tenancy usage-report
#
# Note - the only values you need to change is <group name>. No not change the OCID in the first statement

usage_report_namespace = 'bling'

# Update these values to match the location you want to put the reports
destination_path = 'downloaded_reports'
config_path = 'config/config'

# Make a directory to receive reports
if not os.path.exists(destination_path):
    os.mkdir(destination_path)

# Get the list of usage reports. You download reports from your home region.
config = oci.config.from_file(config_path,'DEFAULT')
usage_report_bucket = config['tenancy']
object_storage = oci.object_storage.ObjectStorageClient(config)
report_bucket_objects = object_storage.list_objects(usage_report_namespace, usage_report_bucket,\
                                                    fields='name,size,timeCreated',\
                                                    start='')

for o in report_bucket_objects.data.objects:
    print('Found file ' + o.name + ' (' + str(o.size / 1024) + ' kb) - created ' + str(o.time_created))
    object_details = object_storage.get_object(usage_report_namespace, usage_report_bucket, o.name)
    filename = o.name.rsplit('/', 1)[-1]

    with open(destination_path + '/' + filename, 'wb') as f:
        for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
            f.write(chunk)

    print('Finished downloading ' + o.name + '\n')
