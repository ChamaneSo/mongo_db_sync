from transformers.accounts_transformer import AccountsTransformer
from writers.accounts_writer import AccountsWriter

class AccountsWatcher():
    def __init__(self):
        self.tranformer = AccountsTransformer()
        self.writer = AccountsWriter()
    
    def watch(self,change):

        if change["operationType"] == "insert":
            data = self.tranformer.transform_for_insert(change["fullDocument"])
            self.writer.write_for_insert(data)

        if change["operationType"] == "update":
            data = self.tranformer.transform_for_update(change["documentKey"]["_id"],change["updateDescription"]["updatedFields"])
            self.writer.write_for_update(change["documentKey"]["_id"],data)

        if change["operationType"] == "delete":
            self.writer.write_for_delete(change["documentKey"]["_id"])