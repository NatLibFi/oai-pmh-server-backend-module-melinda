/**
*
* @licstart  The following is the entire license notice for the JavaScript code in this file.
*
* Melinda backend module for oai-pmh-server
*
* Copyright (C) 2017 University Of Helsinki (The National Library Of Finland)
*
* This file is part of oai-pmh-server-backend-module-melinda
*
* oai-pmh-server-backend-module-melinda program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* oai-pmh-server-backend-module-melinda is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
* @licend  The above is the entire license notice
* for the JavaScript code in this file.
*
*/

/* eslint-disable no-unused-vars */

'use strict';

import fetch from 'node-fetch';
import MarcRecord from 'marc-record-js';
import {MARCXML as MARCXMLConverter} from 'marc-record-serializers';
import * as prototypeFactory from '@natlibfi/oai-pmh-server-backend-module-prototype';
import {readEnvironmentVariable, createToken, parseToken} from './utils';

const DATASTORE_RECORDS_LIMIT = readEnvironmentVariable('DATASTORE_RECORDS_LIMIT', 1000);

export default function factory(options = {}) {
	return {
		getCapabilities: async () => {
			return {
				deletedRecordsSupport: prototypeFactory.DELETED_RECORDS_SUPPORT.PERSISTENT,
				harvestingGranularity: prototypeFactory.HARVESTING_GRANULARITY.DATETIME,
				earliestDatestamp: new Date() // Get the 'stamp from datastore
			};
		},
		getMetadataFormats: async (identifier = undefined) => {
			return [prototypeFactory.METADATA_FORMAT_MARC21];
		},
		getSets: async (resumptionToken = undefined) => {
			throw resumptionToken ? prototypeFactory.ERRORS.badResumptionToken : prototypeFactory.ERRORS.noSetHierarchy;
		},
		getRecord,
		getRecords,
		getIdentifiers
	};
	
	async function getRecord(identifier, metadataPrefix) {
		validateMetadataPrefix(metadataPrefix);
		const response = await fetch(`${options.url}/record/${options.base}/${identifier}?includeMetadata=1`);
		
		if (response.status === 200) {
			const data = await response.json();
			return {
				timestamp: data.recordTimestamp,
				record: MARCXMLConverter.toMARCXML(new MarcRecord(data.record))
			};
		}
		switch (response.status) {
			case 400:
			throw Object.assign(new Error(), {
				errors: [prototypeFactory.ERRORS.badArgument]
			});
			case 404:
			throw Object.assign(new Error(), {
				errors: [prototypeFactory.ERRORS.idDoesNotExist]				
			});
			default:
			throw new Error();
		}
	}
	
	async function getRecords(parameters) {
		
	}
	
	async function getIdentifiers(parameters) {
		validateMetadataPrefix(parameters.metadataPrefix);
		
		if (parameters.resumptionToken) {
			const {tableName, offset} = parseToken(parameters.resumptionToken);
			const response = await fetch(`${options.url}/records/${options.base}?tempTable=${tableName}&offset=${offset}&limit=${DATASTORE_RECORDS_LIMIT}&includeMetadata=1&metadataOnly=1`);
			
			if (response.status === 200) {
				const data = await response.json();
				
				if (data.results.length > 0) {
					const formattedData = {};
					
					if (data.tempTable) {
						formattedData.token = createToken(data.tempTable, offset + DATASTORE_RECORDS_LIMIT);
					}
					
					return Object.assign(formattedData, {
						records: data.results.map(record => {
							return {
								header: [
									{
										identifier: record.id
									},
									{
										datestamp: record.recordTimestamp
									}
								]
							};
						})
					});
				}
				
				throw Object.assign(new Error(), {
					errors: [prototypeFactory.ERRORS.noRecordsMatch]
				});				
			} else if (response.status === 400) {
				throw Object.assign(new Error(), {
					errors: [prototypeFactory.ERRORS.badResumptionToken]
				});
			} else {
				throw new Error();
			}
		} else {console.log(`${options.url}/records/${options.base}?limit=${DATASTORE_RECORDS_LIMIT}&includeMetadata=1&metadataOnly=1${parseParameters(parameters)}`);
			const response = await fetch(`${options.url}/records/${options.base}?limit=${DATASTORE_RECORDS_LIMIT}&includeMetadata=1&metadataOnly=1${parseParameters(parameters)}`);
			if (response.status === 200) {
				const data = await response.json();
								
				if (data.results.length > 0) {
					const formattedData = {};
					
					if (data.tempTable) {
						formattedData.token = createToken(data.tempTable, DATASTORE_RECORDS_LIMIT);
					}
					
					return Object.assign(formattedData, {
						records: data.results.map(record => {
							return {
								header: [
									{
										identifier: record.id
									},
									{
										datestamp: record.recordTimestamp
									}
								]
							};
						})
					});
				}
				
				throw Object.assign(new Error(), {
					errors: [prototypeFactory.ERRORS.noRecordsMatch]					
				});
			} else {
				throw new Error();
			}
		}
	}
	
	function parseParameters(parameters) {
		return Object.keys(parameters).reduce((product, key) => {
			switch (key) {
				case 'from':
				return `${product}&startTime=${parameters[key]}`;
				case 'until':
				return `${product}&endTime=${parameters[key]}`;
				default:
				return product;
			}
		}, '');
	}
	
	function validateMetadataPrefix(prefix) {
		if (prefix !== prototypeFactory.METADATA_FORMAT_MARC21.prefix) {
			throw Object.assign(new Error(), {
				errors: [prototypeFactory.ERRORS.cannotDisseminateFormat]			
			});
		}
	}
}
