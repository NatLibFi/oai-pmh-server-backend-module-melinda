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
import HttpStatus from 'http-status-codes';
import MarcRecord from 'marc-record-js';
import {MARCXML as MARCXMLConverter} from 'marc-record-serializers';
import * as prototypeFactory from '@natlibfi/oai-pmh-server-backend-module-prototype';
import {readEnvironmentVariable, createToken, parseToken, convertFromOaiIdentifier, convertToOaiIdentifier} from './utils';

const DATASTORE_RECORDS_LIMIT = readEnvironmentVariable('DATASTORE_RECORDS_LIMIT', 1000);

export default function (options = {}) {
	const convertLowTagsToSets = record => record.lowTags ? {sets: record.lowTags.map(tag => `library:${tag}`)} : {};
	const validateMetadataPrefix = prefix => {
		if (prefix !== prototypeFactory.METADATA_FORMAT_MARC21.prefix) {
			throw Object.assign(new Error(), {
				errors: [prototypeFactory.ERRORS.cannotDisseminateFormat]
			});
		}
	};
	
	const getRecordData = async (parameters, includeRecordData = false) => {
		validateMetadataPrefix(parameters.metadataPrefix);
		
		const convertResults = data => data.map(item => Object.assign(
			{
				identifier: convertToOaiIdentifier(item.id),
				timestamp: item.recordTimestamp
			},
			convertLowTagsToSets(item),
			includeRecordData ? {data: MARCXMLConverter.toMARCXML(new MarcRecord(item.record))} : {},
		));
		
		const parseParameters = parameters => Object.keys(parameters).reduce((product, key) => {
			switch (key) {
				case 'from':
				return `${product}&startTime=${parameters[key]}`;
				case 'until':
				return `${product}&endTime=${parameters[key]}`;
				case 'set':					
				return [].concat(parameters.set)
				.filter(set => /^library:/.test(set))
				.map(set => set.replace(/^library:/, ''))
				.reduce((product, item) => `${product}&low=${item}`, product);
				default:
				return product;
			}
		}, '');
		
		if (parameters.resumptionToken) {
			let data;
			let formattedData = {};
			const {tableName, offset} = parseToken(parameters.resumptionToken);
			const response = await fetch(`${options.url}/records/${options.base}?tempTable=${tableName}&offset=${offset}&limit=${DATASTORE_RECORDS_LIMIT}${includeRecordData ? '&includeMetadata=1' : '&metadataOnly=1'}`);
			
			switch (response.status) {
				case HttpStatus.OK:
				data = await response.json();
				
				if (data.results.length === DATASTORE_RECORDS_LIMIT) {
					formattedData.resumption = {
						token: createToken(data.tempTable, data.offset + DATASTORE_RECORDS_LIMIT),
						totalLength: data.totalLength,
						offset: data.offset
					};
				}
				
				return Object.assign(formattedData, {records: convertResults(data.results)});
				
				case HttpStatus.NOT_FOUND:
				throw Object.assign(new Error(), {
					errors: [prototypeFactory.ERRORS.noRecordsMatch]
				});
				case HttpStatus.BAD_REQUEST:
				throw Object.assign(new Error(), {
					errors: [prototypeFactory.ERRORS.badResumptionToken]
				});
				default:
				throw new Error();
			}
		} else {
			let data;
			let formattedData = {};
			const response = await fetch(`${options.url}/records/${options.base}?limit=${DATASTORE_RECORDS_LIMIT}${includeRecordData ? '&includeMetadata=1' : '&metadataOnly=1'}${parseParameters(parameters)}`);

			switch (response.status) {
				case HttpStatus.OK:
				data = await response.json();				
				
				if (data.tempTable) {
					formattedData.resumption = {
						token: createToken(data.tempTable, data.offset + DATASTORE_RECORDS_LIMIT),
						totalLength: data.totalLength,
						offset: data.offset
					};
				}
				
				return Object.assign(formattedData, {records: convertResults(data.results)});
				case HttpStatus.NOT_FOUND:			
				throw Object.assign(new Error(), {
					errors: [prototypeFactory.ERRORS.noRecordsMatch]
				});
				case HttpStatus.BAD_REQUEST:
				throw Object.assign(new Error(), {
					errors: [prototypeFactory.ERRORS.badArgument]
				});
				default:			
				throw new Error();
			}
		}
	};
	
	const getEarliestRecordTimestamp = async () => {
		const response = await fetch(`${options.url}/records/${options.base}/timestamps/earliest`);
		if (response.status === HttpStatus.OK) {
			const data = await response.json();
			return data.timestamp;
		}
		throw new Error(`Fetching earliest record timestamp failed: ${response.status}`);
	};
	
	const getRecord = async (identifier, metadataPrefix) => {
		let data;
		validateMetadataPrefix(metadataPrefix);
		const response = await fetch(`${options.url}/record/${options.base}/${convertFromOaiIdentifier(identifier)}?includeMetadata=1`);
		
		switch (response.status) {
			case 200:
			data = await response.json();
			return Object.assign({
				identifier: convertToOaiIdentifier(data.id),
				timestamp: data.recordTimestamp,
				data: MARCXMLConverter.toMARCXML(new MarcRecord(data.record))
			}, convertLowTagsToSets(data));
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
	};
	
	const getSets = async resumptionToken => {
		if (resumptionToken) {
			throw Object.assign(new Error(), {
				errors: [prototypeFactory.ERRORS.badResumptionToken]
			});
		}
		
		const response = await fetch(`${options.url}/records/${options.base}/lowtags`);
		if (response.status === HttpStatus.OK) {
			const data = await response.json();
			return data.map(lowTag => ({ spec: `library:${lowTag}`, name: lowTag }));
		}
		
		throw new Error(`Fetching low tags failed: ${response.status}`);		
	};
	
	const getIdentifiers = async parameters => getRecordData(parameters);
	const getRecords = async parameters => getRecordData(parameters, true);
	const getMetadataFormats = async identifier => [prototypeFactory.METADATA_FORMAT_MARC21];
	const getCapabilities = async () => ({
		deletedRecordsSupport: prototypeFactory.DELETED_RECORDS_SUPPORT.PERSISTENT,
		harvestingGranularity: prototypeFactory.HARVESTING_GRANULARITY.DATETIME,
		earliestDatestamp: await getEarliestRecordTimestamp()
	});
	
	return {
		getCapabilities,
		getMetadataFormats,
		getSets,
		getRecord,
		getIdentifiers,
		getRecords
	};
}
