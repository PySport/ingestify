import flask
from flask import Response, request
from flask_restful import Resource, Api


# Dataset
from ingestify.infra.serialization import serialize
from ingestify.main import get_datastore


def create_server(config_file: str):
    app = flask.Flask(__name__)
    api = Api(app, prefix="/api")

    # @api.representation('application/json')
    # def output_json(data, code, headers=None):
    #     resp = make_response(json.dumps(data, cls=DecimalEncoder), code)
    #     resp.headers.extend(headers or {})
    #     return resp

    datastore = get_datastore(
        config_file
    )

    class DatasetResource(Resource):
        def patch(self, bucket: str, dataset_id: str):
            # TODO: Filter out dataset from body
            return "OK"

        def delete(self, bucket: str, dataset_id: str):
            pass

    class DatasetListResource(Resource):
        def get(self, bucket: str):
            return serialize(
                datastore.get_dataset_collection(
                    #bucket=bucket
                )
            )

    class FileResource(Resource):
        def get(self, bucket, dataset_id: str, version: str, filename: str):
            return Response(
                datastore.load_content(
                    bucket,
                    dataset_id,
                    version,
                    filename
                ).read()
            )

        def put(self, bucket, dataset_id: str, version: str, filename: str):
            return Response(
                datastore.save_content(
                    bucket,
                    dataset_id,
                    version,
                    filename,
                    request.stream
                )
            )

    api.add_resource(DatasetListResource, "/buckets/<string:bucket>/datasets", methods=["GET"])
    api.add_resource(DatasetResource, "/buckets/<string:bucket>/datasets/<string:dataset_id>",
                     methods=["PATCH", "DELETE"])
    api.add_resource(FileResource, "/buckets/<string:bucket>/"
                                   "datasets/<string:dataset_id>/files/"
                                   "<string:version>/<string:filename>",
                     methods=["GET", "PUT"])

    return app


if __name__ == "__main__":
    app = create_server(
        config_file="../examples/statsbomb/config_local.yaml"
    )
    app.run(
        host='0.0.0.0',
        port=8080
    )
