from transformers.payouts_transformer import PayoutsTransformer
from writers.payouts_writer import PayoutsWriter

class PayoutsWatcher:
    def __init__(self):
        self.transformer = PayoutsTransformer()
        self.writer = PayoutsWriter()

    def watch(self, change):
        
        if change["operationType"] == "insert":
            data = self.transformer.transform_for_insert(change["fullDocument"])
            self.writer.write_for_insert(data)

        elif change["operationType"] == "update":
            data = self.transformer.transform_for_update(change["updateDescription"]["updatedFields"])
            self.writer.write_for_update(change["documentKey"]["_id"], data)

        elif change["operationType"] == "delete":
            self.writer.write_for_delete(change["documentKey"]["_id"])