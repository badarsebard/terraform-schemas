import os
import shutil
import time
from math import inf
import multiprocessing
import logging
from python_terraform import Terraform, IsFlagged
import requests
import tempfile

# logger = multiprocessing.log_to_stderr()
# logger.setLevel(logging.DEBUG)


def create_tf_config(name, version):
    return f"""
terraform {{
  required_providers {{
    aws = {{
      source = "{name}"
      version = "{version}"
    }}
  }}
}}"""


def generate_schema(queue: multiprocessing.Queue):
    full_name: str
    version: str
    tier: str
    while not queue.empty():
        full_name, version, tier = queue.get()
        file_name = full_name.replace("/", "-")
        # create temp dir
        cwd = os.getcwd()
        tf_dir = tempfile.mkdtemp(dir=os.path.join(cwd, 'tf_work_dir'))
        # write config
        with open(os.path.join(tf_dir, "main.tf"), 'w') as f:
            f.write(create_tf_config(full_name, version))
        # tf init
        t = Terraform(working_dir=tf_dir)
        ret_code, _, err = t.init(no_color=IsFlagged)
        if ret_code != 0:
            with open(os.path.join(cwd, "schemas", tier, file_name + ".err.log"), 'w') as f:
                f.write(err)
                continue
        # tf providers schema -json
        ret_code, out, err = t.cmd('providers schema', json=IsFlagged, no_color=IsFlagged)
        # write the schemas dir
        if ret_code == 0:
            with open(os.path.join(cwd, "schemas", tier, file_name+".json"), 'w') as f:
                f.write(out)
        else:
            with open(os.path.join(cwd, "schemas", tier, file_name + ".err.log"), 'w') as f:
                f.write(err)
        # rm temp dir
        try:
            shutil.rmtree(tf_dir)
        except OSError as e:
            print(f"Error: {tf_dir} - {e.strerror}")
        print(queue.qsize())


def get_provider_latest_version(provider_id):
    provider_url = f"""https://registry.terraform.io/v2/providers/{provider_id}?include=provider-versions"""
    r = requests.get(provider_url)
    latest = ""
    data = r.json()
    for version in data['included']:
        if (ver := version['attributes']['version']) > latest:
            latest = ver
    return latest


def get_provider_tier(tier, queue):
    registry_url = f"https://registry.terraform.io/v2/providers?filter[tier]={tier}&page[number]=1&page[size]=100"
    r = requests.get(registry_url)
    data = r.json()
    total_pages = data['meta']['pagination']['total-pages']
    all_pages = [(tier, i) for i in range(total_pages)]
    with multiprocessing.Pool() as pool:
        q_items = pool.map(get_registry_page, all_pages)
    q_items = [item for sublist in q_items for item in sublist]
    for i in q_items:
        queue.put(i)


def get_registry_page(tier_page):
    tier, page = tier_page
    registry_url = f"https://registry.terraform.io/v2/providers?filter[tier]={tier}&page[number]={page}&page[size]=100"
    r = requests.get(registry_url)
    data = r.json()
    ret_list = []
    for provider in data['data']:
        full_name = provider['attributes']['full-name']
        version = get_provider_latest_version(provider['id'])
        ret_list.append((full_name.lower(), version, tier))
    return ret_list


def main():
    pqueue = multiprocessing.Queue()
    for t in ['official', 'partner', 'community']:
        get_provider_tier(t, pqueue)
    for _ in range(multiprocessing.cpu_count()):
        multiprocessing.Process(target=generate_schema, args=(pqueue,)).start()
    while not pqueue.empty():
        time.sleep(1)


if __name__ == '__main__':
    main()
