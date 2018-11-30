import tableauserverclient as TSC
from tableauserverclient import ConnectionCredentials, ConnectionItem
import logging
import os

USERNAME='thssbdc'
PASSWORD='thssbdc'
SERVER_URL='166.111.7.244:8000'

tableau_auth = TSC.TableauAuth(USERNAME, PASSWORD)
server = TSC.Server('http://' + SERVER_URL, use_server_version=True)

view_name='Query operation cost time histogram ms'
filepath='./test.png'


def main():
    # The new endpoint was introduced in Version 2.5
    server.version = "2.8"

    with server.auth.sign_in(tableau_auth):
        # req = TSC.RequestOptions()
        #
        # req.filter.add(TSC.Filter("progress", TSC.RequestOptions.Operator.LessThanOrEqual, 0))
        # for job in TSC.Pager(server.jobs, request_opts=req):
        #     print(server.jobs.cancel(job.id), job.id, job.status, job.type)

        all_workbooks, pagination_item = server.workbooks.get()
        for workbook in all_workbooks:
            if workbook.name == 'Query operation cost time histogram ms':
                query_workbook = workbook
        print(query_workbook.name, query_workbook.id)

        # Get the workbook by its Id to make sure it exists
        # resource = server.workbooks.get_by_id(query_workbook.id)

        # trigger the refresh, you'll get a job id back which can be used to poll for when the refresh is done
        # results = server.workbooks.refresh(workbook_id=query_workbook.id)
        # print(results)

        # all_datasources, pagination_item = server.datasources.get()
        # print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
        # for datasource in all_datasources:
        #     if datasource.name == 'auto_test' :
        #         auto_test_datasource = datasource
        #
        # print(auto_test_datasource.name)
        #
        # # trigger the refresh, you'll get a job id back which can be used to poll for when the refresh is done
        # results = server.datasources.refresh(auto_test_datasource)
        # print(results)

        # Step 2: Query for the view that we want an image of
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals, view_name))
        all_views, pagination_item = server.views.get(req_option)
        if not all_views:
            raise LookupError("View with the specified name was not found.")
        view_item = all_views[0]
        # server.views.update()
        # Step 3: Query the image endpoint and save the image to the specified location
        image_req_option = TSC.ImageRequestOptions(imageresolution=TSC.ImageRequestOptions.Resolution.High)
        server.views.populate_image(view_item, image_req_option)
        with open(filepath, "wb") as image_file:
            image_file.write(view_item.image)
        print("View image saved to {0}".format(filepath))


if __name__ == '__main__':
    main()

