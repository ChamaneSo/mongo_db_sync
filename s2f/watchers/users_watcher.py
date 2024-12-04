
from transformers.users_transformer import UsersTransformer
from writers.users_writer import UsersWriter

class UsersWatcher():
    def __init__(self):
        self.tranformer = UsersTransformer()
        self.writer = UsersWriter()
    
    def watch(self,change):

        if change["operationType"] == "insert":
            data, data_provider_user = self.tranformer.transorm_for_insert(change["fullDocument"])
            self.writer.write_for_insert(data, data_provider_user)

        if change["operationType"] == "update":
            data, data_provider_user = self.tranformer.transorm_for_update(change["documentKey"]["_id"],change["updateDescription"]["updatedFields"])
            self.writer.write_for_update(change["documentKey"]["_id"] ,data, data_provider_user)

        if change["operationType"] == "delete":
            self.writer.write_for_delete(change["documentKey"]["_id"])
        