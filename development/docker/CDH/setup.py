from cm_api.api_client import ApiResource

def wait_for_url(url):
   while True:
       try:
           response = urllib2.urlopen(url ,timeout=1)
           break
       except urllib2.URLError:
           time.sleep(5)
           pass

wait_for_url('http://localhost:7180/cmf')

api = ApiResource('localhost', server_port=7180, username='admin', password='admin', use_tls=False, version=11)
cluster = api.get_all_clusters()[0]
services = cluster.get_all_services()

for service in services:
  if 'hive' in service.name.lower():
    service.update_config({'hbase_service':"none"})
  if 'impala' in service.name.lower():
    service.update_config({'hbase_service':"none"})

for service in services:
    if 'hbase' in service.name.lower():
        service.stop().wait()
        cluster.delete_service(service.name)
    if 'solr' in service.name.lower():
        service.stop().wait()
        cluster.delete_service(service.name)
    if 'ks_indexer' in service.name.lower():
        service.stop().wait()
        cluster.delete_service(service.name)


cluster.restart(restart_only_stale_services=False).wait()


for service in services:
  if 'hive' in service.name.lower():
    hive = service


for role in hive.get_all_roles():
  if 'HIVESERVER2' in role.name:
    hostid = role.hostRef.hostId

hive.create_role('WebHCat', 'WEBHCAT', hostid)
hive.restart().wait()