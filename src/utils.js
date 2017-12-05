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
export function readEnvironmentVariable(name, defaultValue, opts) {
	if (process.env[name] === undefined) {
		if (defaultValue === undefined) {
			const message = `Mandatory environment variable missing: ${name}`;
			console.error(message);
			throw new Error(message);
		}
		const loggedDefaultValue = opts && opts.hideDefaultValue ? '[hidden]' : defaultValue;
		console.log(`No environment variable set for ${name}, using default value: ${loggedDefaultValue}`);
	}

	return process.env[name] || defaultValue;
}

export function createToken(tableName, offset) {
	const buffer = Buffer.from(`${offset}:${tableName.replace(/^temp/, '')}`);
	return buffer.toString('base64');
}

export function parseToken(token) {
	const buffer = Buffer.from(token, 'base64');
	return buffer.toString('utf8').split(/:/).reduce((product, value, index) => {
		return Object.assign(product, index === 0 ? {offset: value} : {tableName: `temp${value}`});
	}, {});
}
