import sys
from mongo_connector.doc_managers.elastic2_doc_manager import DocManager as ElasticDocManager
from pymongo import MongoClient
from bson.objectid import ObjectId
import pydash as _
import pprint
import json

get_flag = lambda flag: sys.argv[sys.argv.index(flag)+1]
db_name = "edulasticv2"

mongodb_url = get_flag("-m")
client = MongoClient(mongodb_url)
db = client[db_name]



sample_ids = [ObjectId("39c9cc19c42b559354f3f4d6"),ObjectId("bc8ea5c8f2f13b19113e5ac0"),ObjectId("6d699b4e056950208c855b42"),ObjectId("527fe92af3e49ca71fe26958"),ObjectId("7b96f551871ffc82157bc1eb")]


##### hash indexes related fns

pluck = lambda dict, *args: (dict[arg] for arg in args)

def get_standard_ids(alignment):
    selected_curriculum = _.filter(alignment,lambda x: not x.get("isEquivalentStandard",False))
    standard_ids = []
    for curriculum in selected_curriculum:
        for domain in curriculum["domains"]:
            for standard in domain["standards"]:
                standard_ids.append(str(standard["id"]))
    return standard_ids

def index_data(db):
    store = {}
    for row in db.Standards.find({}):
        if "tloId" in row:
            row["tloId"] = str(row["tloId"])
        store[str(row["_id"])] = row
    return store

def index_curriculum(db):
    store = {}
    for row in db.Curriculums.find({},{'_id':1,'curriculum':1,'subject':1}):
        _id,curriculum,subject = pluck(row,'_id','curriculum','subject')
        store[str(_id)] = {'subject':subject,'curriculum':curriculum}
    return store

def index_equivalent_standard_ids(db):
    store = {}
    for row in db.MultiStandardMappings.find({},{'standardId':1, 'equivalentStandards':1}):
        standard_id,equivalent_standards = pluck(row,'standardId','equivalentStandards')
        store[str(standard_id)] = [str(x["standardId"]) for x in equivalent_standards]
    return store

### hash index related fns end

def expand_standards(ids,store,curriculum_index, equivalent_index):
    all_ids = [str(x) for x in ids]
    for _id in ids:
        if(str(_id) in equivalent_index):
            all_ids = all_ids + equivalent_index[str(_id)]
    all_ids = _.uniq(all_ids) 
    primary_elos = [store[str(x)] for x in all_ids]
    primary_elos_grouped = _.group_by(primary_elos,"tloId")
    domains = []
    result = []

    for elo_id in primary_elos_grouped:
        if "eloId" in primary_elos_grouped[elo_id][0]:
            #sub elo has `eloId`
            #TODO: handle sub elos when implemented in ui
            # currently I couldn't select sub elos from user interface
            pass
        corresponding_tlo = store[elo_id]
        print('corresponding tlo',corresponding_tlo)
        standards = [{**_.pick(x,"level","_id","description","grades"),"name":x["identifier"]} for x in primary_elos_grouped[elo_id]]
        domain = {**_.pick(corresponding_tlo,"_id","description"),"name":corresponding_tlo["identifier"]}
        domain['standards'] = standards
        domain['curriculumId'] = str(corresponding_tlo['curriculumId'])
        domains.append(domain)
    
    domains_grouped = _.group_by(domains,'curriculumId')
    for curriculum_id in domains_grouped:
        curriculum_item = curriculum_index[curriculum_id]
        curriculum_obj = {'curriculmId': curriculum_id, 'subject':curriculum_item['subject'],'curriculum':curriculum_item['curriculum'],'domains':domains_grouped[curriculum_id] }
        result.append(curriculum_obj)
    return result

def transform_testItem(doc,store,curriculum_index,equivalent_index):

    if "$set" in doc and "data" in doc['$set']:
        #print('doc',doc)
        questions = doc['$set']["data"]["questions"]
        doc['$set'].pop('curriculums',None)
    elif "data" in doc:
        doc.pop('curriculums',None)
        questions = doc["data"]["questions"]
    else:
        print('doc None',doc)
        return None

    for question in questions:
        if isinstance(question['alignment'],dict):
            print('alignment expanding',question['alignment'])
            # question['alignment'] = expand_standards(question["alignment"]["standardIds"],store,curriculum_index, equivalent_index)
            question['alignment'] = expand_standards(get_standard_ids(question["alignment"]),store,curriculum_index, equivalent_index)
        else:
            pass
            #print('alignment not expan',question['alignment'])
    

class DocManager(ElasticDocManager):

    def __init__(self,url,**kwargs):
        print("initing....",url,mongodb_url)
        self.indexed_data = index_data(db)
        self.equivalent_index = index_equivalent_standard_ids(db)
        self.curriculum_index = index_curriculum(db)
        print("indexed data done....")
        #pprint.pprint(expand_standards(sample_ids,self.indexed_data, self.curriculum_index,self.equivalent_index))
        super().__init__(url,**kwargs)

    def upsert(self, doc, namespace, timestamp, update_spec=None):
        if namespace ==  ("%s.TestItems" % db_name) and not update_spec:
            transform_testItem(doc,self.indexed_data,self.curriculum_index,self.equivalent_index)
        print('upserting doc',doc,'ns',namespace,'ts',timestamp)
        super().upsert(doc,namespace,timestamp, update_spec)


    def bulk_upsert(self,docs, namespace, timestamp):
        if namespace ==  ("%s.TestItems" % db_name):
            for doc in docs:
                transform_testItem(doc,self.indexed_data,self.curriculum_index,self.equivalent_index)
        print('bulk upsert docs',docs)
        super().bulk_upsert(docs,namespace,timestamp)

    def update(self,document_id, update_spec, namespace, timestamp):
        if namespace ==  ("%s.TestItems" % db_name):
            transform_testItem(update_spec,self.indexed_data,self.curriculum_index,self.equivalent_index)
        print('updating id',document_id,'updateSpec',update_spec,':ns:,',namespace,':ts:,', timestamp)
        super().update(document_id,update_spec,namespace,timestamp)

    def remove(self, document_id, namespace, timestamp):
        print('removing id ',document_id)
        super().remove(document_id,namespace,timestamp)
