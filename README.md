# Appwrite tools

## Installation

1. `git clone` this repository
2. `npm install`

## I migrated from 0.11 to 0.12 and I don't like attributes it converted my rules into

For example, every numeric value from 0.11 was converted to float, as this makes the most sense. But what if you really need integer there? You cannot remove and create new attribute, right? That would remove all data from documents of that attribute...

This set of tools will allow you to backup, wipe and restore documents from your database.

1. Backup all documents from this collection into CSV file:

```
$ node index.js --action documents-backup --endpoint https://demo.appwrite.io/v1 --project 6190f1783c600 --collection 6190fb24b7748 --api-key f73fef25cf59a1ee6d...23446425643534
```

> Make sure to replace code snippet with your endpoint, project ID, collection ID and API key.

2. Wipe (delete) all documents from this collection:

```
$ node index.js --action documents-wipe --endpoint https://demo.appwrite.io/v1 --project 6190f1783c600 --collection 6190fb24b7748 --api-key f73fef25cf59a1ee6d...23446425643534
```

> Only action name has changed. Make sure to replace code snippet with your endpoint, project ID, collection ID and API key.

3. Manually delete collection, create new one and setup attributes as required. Make sure to keep same attributes, just change type or length, as needed. At this point, you can also setup indexes

4. Restore all documents from backup file back to Appwrite collection:

```
$ node index.js --action documents-restore --file backup_1641325131454.csv --endpoint https://demo.appwrite.io/v1 --project 6190f1783c600 --collection 6190fb24b7748 --api-key f73fef25cf59a1ee6d...23446425643534
```

> You need to change file name from `backup_1641325131454.csv` to whatever your ste 1 created. File name is unique each time you run it. Also, make sure to replace code snippet with your endpoint, project ID, collection ID and API key.

Done 🥳 You have just backed-up and restored documents from your Appwrite collection allowing you to change attribute types, min/max, length or defaults without losing or converting any data.
