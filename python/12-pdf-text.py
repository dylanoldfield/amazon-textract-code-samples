import boto3
import time
import webbrowser, os
import json
import webbrowser, os
import io
from io import BytesIO
import sys
import csv
from pprint import pprint

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}

                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '
    return text




def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    table_id = 'Table_' + str(table_index)

    # get cells.
    csv = 'Table: {0}\n\n'.format(table_id)

    for row_index, cols in rows.items():

        for col_index, text in cols.items():
            csv += '{}'.format(text) + ","
        csv += '\n'

    csv += '\n\n\n'
    return csv

def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_text_detection(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        }
    })

    return response["JobId"]

def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []

    time.sleep(5)

    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)

    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)

        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

# Document
def main():
    s3BucketName = "amazon-texttract-cfaflashcard"
    documentName = "Flashcards_List_cropped.pdf"
    jobId = startJob(s3BucketName, documentName)
    print("Started job with id: {}".format(jobId))
    if(isJobComplete(jobId)):
        response = getJobResults(jobId)
        #print(response)
        # Print detected text
        blocks_map = {}
        table_blocks = []
        definitions = []
        counter = 0
        key = ''
        value = ''
        holder = ''

        for resultPage in response:
            for block in resultPage["Blocks"]:
                # Get the text blocks
                blocks_map[block['Id']] = block
                if block['BlockType'] == "TABLE":
                    print("table found.")
                    table_blocks.append(block)

                # since the definitions come in groups of 3, I create a counter to pull the number and the word and its definition
                if block['BlockType'] == "LINE":
                    text = block["Text"]
                    # Check if the line is numeric signifying a new definition
                    if(is_number(text)):
                        if counter > 1:
                            tup = (key,value,holder)
                            definitions.append(tup)
                            print(key + ": "+value)
                            key=''
                            value = ''
                            holder=''
                        counter = 1
                        continue
                    # assings the word, definition and category
                    # word
                    if(counter == 1):
                        key = text
                        counter +=1
                        continue
                    # definition
                    if(counter >= 2):
                        value += " " +text
                        counter +=1
                        continue
                    # # category
                    # if(counter == 3):
                    #     holder = text
                    #     counter +=1
                    #     continue
                    # # remainder of the definition
                    # if(counter > 3):
                    #     value += " " + text
                    #     continue


        if len(table_blocks) <= 0:
            print("No Tables Found")

        print("Tables Processed... Moving to CSV")
        output_csv = ''
        for index, table in enumerate(table_blocks):
            output_csv += generate_table_csv(table, blocks_map, index +1)
            output_csv += '\n\n'

        output_file = 'output.csv'
        # replace content
        with open(output_file, "wt") as fout:
            writer = csv.writer(fout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for tup in definitions:
                writer.writerow(tup)
        # show the results
        print('Done. CSV OUTPUT FILE: ', output_file)
if __name__ == "__main__":
    main()
