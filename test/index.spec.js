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

/* eslint-disable no-undef, max-nested-callbacks, no-unused-expressions */

'use strict';

import {expect} from 'chai';
import * as testContext from '../src/index';

describe('index', () => {
	it('Should export the expected members', () => {
		expect(testContext).to.contain.all.keys(['factory', 'HARVESTING_GRANULARITY', 'DELETED_RECORDS_SUPPORT', 'ERRORS']);
		expect(testContext.HARVESTING_GRANULARITY).to.be.an('object');
		expect(testContext.DELETED_RECORDS_SUPPORT).to.be.an('object');
		expect(testContext.ERRORS).to.be.an('object');
		expect(testContext.factory).to.be.a('function');
	});

	describe('#factory', () => {
		const obj = testContext.factory();

		it('Should return the expected object', () => {
			expect(obj).to
			.respondTo('getCapabilities').and.to
			.respondTo('getRecord').and.to
			.respondTo('getMetadataFormats').and.to
			.respondTo('getSets').and.to
			.respondTo('getIdentifiers').and.to
			.respondTo('getRecords');
		});

		describe('object', () => {
			describe('#getCapabilities', () => {
				it('Should resolve with an instance of capabilities', () => {
					return obj.getCapabilities().then(capabilities => {
						expect(capabilities).to.be.an('object').and.to.contain.all.keys(['deletedRecordsSupport', 'harvestingGranularity', 'earliestDatestamp']);
						expect(capabilities.deletedRecordsSupport).to.be.a('number');
						expect(capabilities.harvestingGranularity).to.be.a('number');
						expect(Object.isPrototypeOf.call(Date.prototype, capabilities.earliestDatestamp)).to.be.true;
					});
				});
			});

			describe('#getRecord', () => {
				it.skip('Should resolve with a record');
				it.skip('Should reject because of invalid arguments');

				it('Should reject because the record could not be found', () => {
					return obj.getRecord('foo', 'bar').catch(err => {
						expect(err).to.equal(testContext.ERRORS.idDoesNotExist);
					});
				});

				it.skip('Should reject because the metadata prefix is not supported');
			});

			describe('#getMetadataFormats', () => {
				it('Should resolve with all supported metadata formats', () => {
					return obj.getMetadataFormats().then(result => {
						expect(result).to.be.an('array');
						expect(result).to.have.length.above(0);
						expect(result[0]).to.be.an('object').and.to.have.all.keys(['prefix', 'schema', 'namespace']);
						expect(result[0].prefix).to.be.a('string');
						expect(result[0].schema).to.be.a('string');
						expect(result[0].namespace).to.be.a('string');
					});
				});

				it.skip('Should resolve with the metadata format of a record specified by identifier');
				it.skip('Should reject because of invalid arguments');

				it('Should reject because the record specified by identifier couldn\'t be found', () => {
					return obj.getMetadataFormats().catch(err => {
						expect(err).to.equal(testContext.ERRORS.idDoesNotExist);
					});
				});

				it.skip('Should reject because no metadata formats could be found for the record specified by identifier');
			});

			describe('#getSets', () => {
				it.skip('Should resolve with sets');
				it.skip('Should resolve with sets and flow control parameters');
				it.skip('Should resolve with sets requested with a resumption token');
				it.skip('Should reject because of invalid arguments');

				it('Should reject because the resumption token is invalid or expired', () => {
					return obj.getSets('foo').catch(err => {
						expect(err).to.equal(testContext.ERRORS.badResumptionToken);
					});
				});

				it('Should reject because the backend doesn\'t support sets', () => {
					return obj.getSets().catch(err => {
						expect(err).to.equal(testContext.ERRORS.noSetHierarchy);
					});
				});
			});

			describe('#getIdentifiers', () => {
				it.skip('Should resolve with identifiers');
				it.skip('Should resolve with identifiers and flow control parameters');
				it.skip('Should resolve with identifiers requested with a resumption token');
				it.skip('Should resolve with identifiers requested with datestamp-based selective harvesting (from)');
				it.skip('Should resolve with identifiers requested with datestamp-based selective harvesting (until)');
				it.skip('Should resolve with identifiers requested with set-based selective harvesting');
				it.skip('Should resolve with identifiers requested with set- and datestamp-based selective harvesting');
				it.skip('Should reject because of invalid arguments');
				it.skip('Should reject because the resumption token is invalid or expired');
				it.skip('Should reject because the metadata format is not supported by the backend');
				it('Should reject because no identifiers were found', () => {
					return obj.getIdentifiers().catch(err => {
						expect(err).to.equal(testContext.ERRORS.noRecordsMatch);
					});
				});
				it.skip('Should reject because the backend doesn\'t support sets');
			});

			describe('#getRecords', () => {
				it.skip('Should resolve with records');
				it.skip('Should resolve with records and flow control parameters');
				it('Should resolve with records requested with a resumption token', () => {
					return obj.getRecords('foo').catch(err => {
						expect(err).to.equal(testContext.ERRORS.noRecordsMatch);
					});
				});
				it.skip('Should resolve with records requested with datestamp-based selective harvesting (from)');
				it.skip('Should resolve with records requested with datestamp-based selective harvesting (until)');
				it.skip('Should resolve with records requested with set-based selective harvesting');
				it.skip('Should resolve with records requested with set- and datestamp-based selective harvesting');
				it.skip('Should reject because of invalid arguments');
				it.skip('Should reject because the resumption token is invalid or expired');
				it.skip('Should reject because the metadata format is not supported by the backend');
				it('Should reject because no records were found', () => {
					return obj.getRecords().catch(err => {
						expect(err).to.equal(testContext.ERRORS.noRecordsMatch);
					});
				});
			});
		});
	});
});
