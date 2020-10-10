import numpy
from nPYc.utilities.io.extractParams import *
from datetime import datetime
import codecs
from nPYc.utilities._conditionalJoin import conditionalJoin
import warnings


def getSampleMetadataFromWatersRawFiles(rawDataPath):
	"""
	Get acquisition metadata from Waters RAW files and returns them as a dataframe

	:param str rawDataPath: Path to folder of raw data
	"""

	# Get the paramters as a table
	instrumentParams = extractParams(rawDataPath, 'Waters .raw')

	# Strip any whitespace from 'Sample File Name'
	instrumentParams['Sample File Name'] = instrumentParams['Sample File Name'].str.strip()

	# Parse acqustion times
	instrumentParams['Acquired Time'] = numpy.nan
	for i in range(instrumentParams.shape[0]):
		try:
			instrumentParams.loc[i, 'Acquired Time'] = datetime.strptime(str(instrumentParams.loc[i, '$$ Acquired Date:']) + " " + str(instrumentParams.loc[i,'$$ Acquired Time:']), '%d-%b-%Y %H:%M:%S')
		except ValueError:
			pass
		
	# Rename '$$ Acquired Time' and '$$ Acquired Date to avoid confusion
	instrumentParams.rename(columns={'$$ Acquired Time:': 'Measurement Time'}, inplace=True)
	instrumentParams.rename(columns={'$$ Acquired Date:': 'Measurement Date'}, inplace=True)
	
	
	##
	# Detect duplicate experiment filenames
	##
	duplicateSamples = instrumentParams.loc[instrumentParams['Sample File Name'].duplicated(keep=False)]
	if duplicateSamples.size > 0:
		warnings.warn('Duplicate raw data loaded, discarding duplicates.', UserWarning)
		# Drop duplicate files
		instrumentParams = instrumentParams.loc[instrumentParams['Sample File Name'].duplicated(keep='first') == False]

	return instrumentParams


def extractWatersRAWParams(filePath, queryItems):
	"""
	Read parameters defined in *queryItems* for Waters .RAW data.

	:param filePath: Path to .RAW folder
	:type filePath: str
	:param dict queryItems: names of parameters to extract values for
	:returns: Dictionary of extracted parameters
	:rtype: dict
	"""

	# Get filename
	filename = os.path.basename(filePath)
	results = dict()
	results['Warnings'] = ''

	results['File Path'] = filePath
	results['Sample File Name'] = os.path.splitext(filename)[0]

	for inputFile in queryItems.keys():
		localPath = os.path.join(filePath, inputFile)
		try:
			f = codecs.open(localPath, 'r', encoding='latin-1')
			contents = f.readlines()

			logging.debug('Searching file: ' + localPath)

			# Loop over the search terms
			for findthis in queryItems[inputFile]:
				logging.debug('Looking for: ' + findthis)
				indices = [i for i, s in enumerate(contents) if findthis in s]
				if indices:
					logging.debug('Found on line: ' + str(indices[0]))
					foundLine = contents[indices[0]]
					logging.debug('Line reads: ' + foundLine.rstrip())
					query = '(' + re.escape(findthis) + ')\W+(.+)\r'
					logging.debug('Regex is: ' + query)
					m = re.search(query, foundLine)
					logging.debug('Found this: ' + m.group(1) + ' and: ' + m.group(2))

					results[findthis.strip()] = m.group(2).strip()
				else:
					results['Warnings'] = conditionalJoin(results['Warnings'],
														  'Parameter ' + findthis.strip() + ' not found.')
					warnings.warn('Parameter ' + findthis + ' not found in file: ' + os.path.join(localPath))

			f.close()
		except IOError:
			for findthis in queryItems[inputFile]:
				results['Warnings'] = conditionalJoin(results['Warnings'],
													  'Unable to open ' + localPath + ' for reading.')
				warnings.warn('Unable to open ' + localPath + ' for reading.')

	return results