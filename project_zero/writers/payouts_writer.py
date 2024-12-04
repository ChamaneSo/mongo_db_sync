from writers.base_writer import BaseWriter
from bson import ObjectId

class PayoutsWriter(BaseWriter):
    def __init__(self):
        super().__init__()
        print("Creating payouts table ...")

        schema = {
            "id" : "VARCHAR(36) DEFAULT NULL",
            "accountId" : "VARCHAR(36) DEFAULT NULL",
            "userId" : "VARCHAR(36) DEFAULT NULL",
            "amount" : "DECIMAL(10, 2) DEFAULT NULL",
            "dataProviderAccountId" : "VARCHAR(36) DEFAULT NULL",
            "dataProviderType" : "VARCHAR(255) DEFAULT NULL",
            "status" : "payout_status_enum DEFAULT NULL",
            "reason" : "VARCHAR(255) DEFAULT NULL",
            "createdAt" : "TIMESTAMP DEFAULT NULL",
            "updatedAt" : "TIMESTAMP DEFAULT NULL"
        }

        self._create_type("payout_status_enum", ["'pending'",  "'readyForAudit'", "'paid'", "'approved'", "'rejected'"])
        self._create_table("payouts", schema)

    def write_for_insert(self, data):
        self._insert("payouts", data)

    def write_for_update(self, id, data):
        id = str(ObjectId(id))
        self._update("payouts", id, data)

    def write_for_delete(self, id):
        self._delete("payouts", str(ObjectId(id)))

