#encoding:UTF-8
import urllib2

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError

from hashlib import sha1
from lxml import html
import re
from datetime import datetime
from pylons import config
from base import HarvesterBase
from lxml.html.clean import Cleaner
import pprint

import logging
log = logging.getLogger(__name__)

from base import HarvesterBase

class NTPCHarvester(HarvesterBase):
    '''
    A Harvester for NTPC instances
    '''

    log.info('initial...')
    config = None

    api_version = 2

    PREFIX_URL = "http://data.ntpc.gov.tw"
    CATALOGUE_INDEX_URL = "/NTPC/od/hot"

    def _get_rest_api_offset(self):
        return '/api/%d/rest' % self.api_version

    def _get_search_api_offset(self):
        return '/api/%d/search' % self.api_version

    def _get_content(self, url):
        log.debug('__get_content()')
        tag = ['br']
        #log.debug('0.01')
        c = Cleaner(remove_tags=tag)
        #log.debug('0.02')
        package_dict = {'extras': {}, 'resources': [], 'tags': []}
        #log.debug(url)
        data = urllib2.urlopen(url)
        #log.debug('0')
        meta = dict()
        doc = html.parse(data)
        #log.debug('0.1')
        c(doc)
        #log.debug('1')
        dataset_title = doc.find(".//div[@class='title']")
        title = dataset_title.text.strip(' \t\n\r')
        msg_count = 0
        key = None
        value = None
        url_table = doc.find(".//table[@frame='border']")
        for tr in url_table.findall("tr"):
            for td in tr.findall("td"):
                for a in td.findall("a"):
                    if a is not None:
                        res = self.PREFIX_URL + a.get("href")
                        if 'JSON' in a.text.strip():
                            package_dict["resources"].append({
                                "url": res,
                                "format": 'JSON',
                                "description": title + ('(JSON)')
                            })
                        if 'csv' in a.text.strip():
                            package_dict["resources"].append({
                                "url": res,
                                "format": 'CSV',
                                "description": title + '(CSV' +u'格式' + ')'
                            })
                        if 'xml' in a.text.strip():
                            package_dict["resources"].append({
                                "url": res,
                                "format": 'XML',
                                "description": title + '(XML' +u'格式' + ')'
                            })
                        if u'原始檔案' in a.text.strip():
                            package_dict["resources"].append({
                                "url": res,
                                "format": 'orgfile',
                                "description": title + '(' + u'原始檔案' +u'格式' + ')'
                            })
        table = doc.find(".//table[@cellpadding='2'][@width='90%']")
        for tr in table.findall("tr"):
            for td in tr.findall("td"):
                if td.text is None:
                    continue
                a = td.find("a")
                if a is not None:
                    key_or_value = a.attrib['href']
                    value = key_or_value.strip(' \t\n\r')
                    meta[key] = value.strip(' \t\n\r')
                    msg_count +=1
                    #log.debug("value:" + value)
                    value = None
                    continue
                key_or_value = td.text.strip(' \t\n\r')
                if msg_count % 2 == 0:
                    key = key_or_value
                    #log.debug("key"+key)
                    msg_count +=1
                else:
                    value = key_or_value
                    msg_count +=1
                    #pprint.pprint("value:" + value)
                    if key is not None and value is not None:
                        #log.debug("key:%s,  value:%s" % (key, value))
                        meta[key] = value
                        key = None
                        value = None
        package_dict["title"] ='NTPC_' + title
        package_dict["author"] = meta[u"提供單位："]
        package_dict["notes"] = meta[u"詮釋資料："]
        #package_dict["Name"] = title

        package_dict["extras"][u"資料集網址"] = url #PREFIX_URL #+ harvest_object.content
        package_dict["extras"][u"分　　類："] = meta[u"分　　類："]
        package_dict["extras"][u"標　　籤："] = meta[u"標　　籤："]
        package_dict["extras"][u"提供單位："] = meta[u"提供單位："]
        package_dict["extras"][u"上架日期："] = meta[u"上架日期："]
        package_dict["extras"][u"更新頻率："] = meta[u"更新頻率："]
        package_dict["extras"][u"更新日期："] = meta[u"更新日期："]
        package_dict["extras"][u"紀錄筆數："] = meta[u"紀錄筆數："]
        package_dict["extras"][u"參考網址："] = meta[u"參考網址："]
        package_dict["extras"][u"欄位定義："] = meta[u"欄位定義："]
        package_dict["extras"][u"紀錄筆數："] = meta[u"紀錄筆數："]

#        if "標　　籤：".decode("utf8") in meta.keys():
#            dataset_meta_list = meta[u"標　　籤："].split(".")
             #package_dict["tags"] = meta[u"標　　籤："].split(u"、")
#            for item in dataset_meta_list:
#                dataset_sub_meta_list = item.split(":")
#                package_dict["tags"].append({
#                    "name": 'aaa',#dataset_sub_meta_list[0].replace('\n', '').replace('\r', '').replace('\t',''),
#                    "dispaly_name": 'bbb'#dataset_sub_meta_list[0].replace('\n', '').replace('\r', '').replace('\t','')
#                })
#                log.debug('get meta: %s' % dataset_sub_meta_list)

        package_dict["license_id"] = "odc-odbl"
        return json.dumps(package_dict,ensure_ascii=False)

    def _get_group(self, base_url, group_name):
        url = base_url + self._get_rest_api_offset() + '/group/' + group_name
        try:
            content = self._get_content(url)
            return json.loads(content)
        except Exception, e:
            raise e

    def _set_config(self,config_str):
        if config_str:
            self.config = json.loads(config_str)
            self.api_version = int(self.config['api_version'])

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    def info(self):
        return {
            'name': 'opendata_ntpc',
            'title': 'NTPC',
            'description': 'Harvests remote NTPC instances',
            'form_config_interface':'Text'
        }

    def validate_config(self,config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'api_version' in config_obj:
                try:
                    int(config_obj['api_version'])
                except ValueError:
                    raise ValueError('api_version must be an integer')

            if 'default_tags' in config_obj:
                if not isinstance(config_obj['default_tags'],list):
                    raise ValueError('default_tags must be a list')

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'],list):
                    raise ValueError('default_groups must be a list')

                # Check if default groups exist
                context = {'model':model,'user':c.user}
                for group_name in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(context,{'id':group_name})
                    except NotFound,e:
                        raise ValueError('Default group not found')

            if 'default_extras' in config_obj:
                if not isinstance(config_obj['default_extras'],dict):
                    raise ValueError('default_extras must be a dictionary')

            if 'user' in config_obj:
                # Check if user exists
                context = {'model':model,'user':c.user}
                try:
                    user = get_action('user_show')(context,{'id':config_obj.get('user')})
                except NotFound,e:
                    raise ValueError('User not found')

            for key in ('read_only','force_all'):
                if key in config_obj:
                    if not isinstance(config_obj[key],bool):
                        raise ValueError('%s must be boolean' % key)

        except ValueError,e:
            raise e

        return config

    def _get_ntpc_dataset_count(self, url):
        data = urllib2.urlopen(url)
        doc = html.parse(data)

        for span in doc.findall('//span[@class="h_bu_03"]'):
            number = ''.join([i for i in span.text_content() if i.isdigit()])
            if number.isdigit():
                return int(number)
        return 0

    def gather_stage(self,harvest_job):
        log.debug('In NTPCHarvester gather_stage (%s)' % harvest_job.source.url)

        url = self.PREFIX_URL + self.CATALOGUE_INDEX_URL
        get_all_packages = True
        try:
            package_ids = []
            dataset_count = self._get_ntpc_dataset_count(url)
            msg_count = 0
            for x in range(dataset_count/10 + 1):
                page_url = url + '?currentPage=%s' % (x + 1)
                data = urllib2.urlopen(page_url)
                doc = html.parse(data)
                for div in doc.findall("//a[@href]"):
                    if '/NTPC/od/query;' in div.attrib['href']:
                        link = div.attrib['href']
                        id = sha1(link).hexdigest()
                        obj = HarvestObject(guid=id, job=harvest_job, content=link)
                        obj.save()
                        package_ids.append(obj.id)
                        msg_count = msg_count + 1

            if msg_count == 0:
                self._save_gather_error('No packages received for URL: %s' % url,
                        harvest_job)
                return None

        except Exception, e:
            self._save_gather_error('%r'%e.message,harvest_job)

        self._set_config(harvest_job.source.config)

        log.debug(' Check if this source has been harvested before')
        # Check if this source has been harvested before
        previous_job = Session.query(HarvestJob) \
                        .filter(HarvestJob.source==harvest_job.source) \
                        .filter(HarvestJob.gather_finished!=None) \
                        .filter(HarvestJob.id!=harvest_job.id) \
                        .order_by(HarvestJob.gather_finished.desc()) \
                        .limit(1).first()

        return package_ids



    def fetch_stage(self,harvest_object):
        log.debug('In NTPCHarvester fetch_stage')

        self._set_config(harvest_object.job.source.config)

        # Get source URL
        url = self.PREFIX_URL + harvest_object.content
        log.debug('url: %s' % url)

        # Get contents
        try:
            content = self._get_content(url)
        except Exception,e:
            self._save_object_error('Unable to get content for package: %s: %r' % \
                                        (url, e),harvest_object)
            return None

        # Save the fetched contents in the HarvestObject
        harvest_object.content = content
        harvest_object.save()
        return True

    def import_stage(self,harvest_object):
        log.debug('In NTPCHarvester import_stage')
        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,
                    harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)


            log.debug(package_dict)
            log.debug('=============================================')
            package_dict["id"] = harvest_object.guid
            # Set default tags if needed
            default_tags = self.config.get('default_tags',[])
            if default_tags:
                if not 'tags' in package_dict:
                    package_dict['tags'] = []
                package_dict['tags'].extend([t for t in default_tags if t not in package_dict['tags']])

            remote_groups = self.config.get('remote_groups', None)
            log.debug(remote_groups)
            log.debug('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            if not remote_groups in ('only_local', 'create'):
                # Ignore remote groups
                package_dict.pop('groups', None)
            else:
                if not 'groups' in package_dict:
                    package_dict['groups'] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []
                context = {'model': model, 'session': Session, 'user': 'harvest'}

                for group_name in package_dict['groups']:
                    log.debug(group_name)
                    try:
                        data_dict = {'id': group_name}
                        group = get_action('group_show')(context, data_dict)
                        if self.api_version == 1:
                            validated_groups.append(group['name'])
                        else:
                            validated_groups.append(group['id'])
                    except NotFound, e:
                        log.info('Group %s is not available' % group_name)
                        if remote_groups == 'create':
                            try:
                                group = self._get_group(harvest_object.source.url, group_name)
                            except:
                                log.error('Could not get remote group %s' % group_name)
                                continue

                            for key in ['packages', 'created', 'users', 'groups', 'tags', 'extras', 'display_name']:
                                group.pop(key, None)
                            get_action('group_create')(context, group)
                            log.info('Group %s has been newly created' % group_name)
                            if self.api_version == 1:
                                validated_groups.append(group['name'])
                            else:
                                validated_groups.append(group['id'])

                package_dict['groups'] = validated_groups

            # Ignore remote orgs for the time being
            package_dict.pop('owner_org', None)

            # Set default groups if needed
            default_groups = self.config.get('default_groups', [])
            if default_groups:
                package_dict['groups'].extend([g for g in default_groups if g not in package_dict['groups']])

            # Find any extras whose values are not strings and try to convert
            # them to strings, as non-string extras are not allowed anymore in
            # CKAN 2.0.
            for key in package_dict['extras'].keys():
                if not isinstance(package_dict['extras'][key], basestring):
                    try:
                        package_dict['extras'][key] = json.dumps(
                                package_dict['extras'][key])
                    except TypeError:
                        # If converting to a string fails, just delete it.
                        del package_dict['extras'][key]

            # Set default extras if needed
            default_extras = self.config.get('default_extras',{})
            if default_extras:
                override_extras = self.config.get('override_extras',False)
                if not 'extras' in package_dict:
                    package_dict['extras'] = {}
                for key,value in default_extras.iteritems():
                    if not key in package_dict['extras'] or override_extras:
                        # Look for replacement strings
                        if isinstance(value,basestring):
                            value = value.format(harvest_source_id=harvest_object.job.source.id,
                                     harvest_source_url=harvest_object.job.source.url.strip('/'),
                                     harvest_source_title=harvest_object.job.source.title,
                                     harvest_job_id=harvest_object.job.id,
                                     harvest_object_id=harvest_object.id,
                                     dataset_id=package_dict['id'])

                        package_dict['extras'][key] = value

            log.debug('_create_or_update_package')
            log.debug(package_dict)
            log.debug(harvest_object)
            result = self._create_or_update_package(package_dict,harvest_object)

            if result and self.config.get('read_only',False) == True:

                package = model.Package.get(package_dict['id'])

                # Clear default permissions
                model.clear_user_roles(package)

                # Setup harvest user as admin
                user_name = self.config.get('user',u'harvest')
                user = model.User.get(user_name)
                pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

                # Other users can only read
                for user_name in (u'visitor',u'logged_in'):
                    user = model.User.get(user_name)
                    pkg_role = model.PackageRole(package=package, user=user, role=model.Role.READER)

            log.debug('import_stage return true')
            return True
        except ValidationError,e:
            self._save_object_error('Invalid package with GUID %s: %r' % (harvest_object.guid, e.error_dict),
                    harvest_object, 'Import')
        except Exception, e:
            self._save_object_error('%r'%e,harvest_object,'Import')

