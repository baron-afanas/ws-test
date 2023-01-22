import json
from datetime import datetime
from lib import Response


class FileHandler:
    def __init__(self):


        self.errors = []

    def schema_validator(self, obj) -> Response:
        try:
            obj["userId"] = str(obj["userId"])
            obj["Id"] = str(obj["Id"] + "ssss")
            obj["RecordDate"] = datetime.strptime(
                obj["RecordDate"], "%Y-%m-%d %H:%M:%S.%f %Z"
            )
            obj["UpdatedAtDate"] = (
                datetime.strptime(obj["UpdatedAtDate"], "%Y-%m-%d %H:%M:%S.%f %Z")
                if obj.get("UpdatedAtDate")
                else None
            )
            obj["Amount"] = float(obj["Amount"])
        except Exception as e:
            return Response(False, f"Faulty primary key: {e}")

        try:
            obj.update(
                {
                    "MerchantId": str(obj["MerchantId"])
                    if obj.get("MerchantId")
                    else "",
                    "MerchantName": str(obj["MerchantName"])
                    if obj.get("MerchantName")
                    else "",
                    "CategoryId": str(obj["CategoryId"])
                    if obj.get("CategoryId")
                    else "",
                    "CategoryName": str(obj["CategoryName"])
                    if obj.get("CategoryName")
                    else "",
                    "SubCategoryId": str(obj["SubCategoryId"])
                    if obj.get("SubCategoryId")
                    else "",
                    "SubCategoryName": str(obj["SubCategoryName"])
                    if obj.get("SubCategoryName")
                    else "",
                    "Subscription": str(obj["Subscription"])
                    if obj.get("Subscription")
                    else "",
                    "RecurrenceFrequency": str(obj["RecurrenceFrequency"])
                    if obj.get("RecurrenceFrequency")
                    else "",
                    "Currency": str(obj["Currency"]) if obj.get("Currency") else "",
                    "Fingerprint": str(obj["Fingerprint"])
                    if obj.get("Fingerprint")
                    else "",
                    "Iban": str(obj["Iban"]) if obj.get("Iban") else "",
                }
            )
        except Exception as e:
            return Response(False, f"Faulty secondary key: {e}")

        return Response(True, f"object valid")

    def file_parser(self) -> Response:
        if not self.data:
            return Response(False, f"No data loaded")

        for idx, element in enumerate(self.data):
            if (
                type(element.get("userId")) is not str
                or type(element.get("Transaction")) is not dict
            ):

                self.errors.append(
                    {
                        "element": self.data.pop(idx),
                        "error": "userId or Transaction missing or malformed",
                    }
                )
                continue

            try:
                element.update(element.pop("Transaction"))
                validation = self.schema_validator(element)
                if validation.success is not True:
                    self.errors.append(
                        {"element": self.data.pop(idx), "error": validation.message}
                    )
                    continue
            except Exception as e:
                self.errors.append({"element": self.data.pop(idx), "error": f"{e}"})
                continue

        return Response(True, f"Parsing & validation finished")

    def file_loader(self, filename) -> Response:
        try:
            with open(filename) as json_file:
                self.data = json.load(json_file)
        except Exception as e:
            return Response(False, f"Error loading the file: {e}")
        if type(self.data) != list:
            return Response(False, "the file schema is not compatible")
        return Response(True, "Success")
