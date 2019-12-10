import time
import random
from cloudant import Cloudant
from cloudant.database import CloudantDatabase
from cloudant.document import Document
from cloudant.error import CloudantException


class CloudantClient:
    max_retries = 15

    def __init__(self, username: str, apikey: str):
        self.client = Cloudant.iam(username, apikey)
        self.client.connect()

    def get_conn(self):
        return self.client

    def put(self, database_name: str, document_id: str, data: dict):
        db = CloudantDatabase(self.client, database_name)
        data = data.copy()
        data['_id'] = document_id
        retry = self.max_retries
        while retry > 0:
            try:
                if not db.exists():
                    db.create()
                doc = Document(db, document_id=document_id)
                if doc.exists():
                    doc.fetch()
                    doc.delete()
                doc = db.create_document(data)
                doc.save()
                break
            except CloudantException as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e

    def get(self, database_name: str, document_id: str = None):
        db = CloudantDatabase(self.client, database_name)
        if not db.exists():
            raise KeyError("Database does not exist")

        retry = self.max_retries
        while retry > 0:
            try:
                if document_id is None:
                    doc = dict()
                    for d in db:
                        task_id = d['_id']
                        del d['_id']
                        del d['_rev']
                        doc[task_id] = d
                else:
                    doc = Document(db, document_id=document_id)
                    if not doc.exists():
                        raise KeyError("Database or document does not exist")
                    doc.fetch()
                break
            except CloudantException as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e
        return dict(doc)

    def delete(self, database_name: str, document_id: str = None):
        db = CloudantDatabase(self.client, database_name)
        retry = self.max_retries
        while retry > 0:
            try:
                if document_id is None and db.exists():
                    db.delete()
                else:
                    doc = Document(db, document_id=document_id)
                    doc.fetch()
                    if doc.exists():
                        doc.delete()
                break
            except CloudantException as e:
                time.sleep(random.random())
                retry -= 1
                if retry == 0:
                    raise e
