import pandas as pd
import numpy as np
import re
import string
import nltk

from datetime import datetime

from daticamera import DatiCamera

class SpeechManipulator():
	"""
	Reads, extracts, cleans and merges data from a session csv and
	a session metadata csv in order to get each speech of each
	session with information like the speaker, her gender, her party and
	the legislature.

	Parameters
	----------
	path_speeches : str
		Path to the csv file containing the sessions raw text
		
	path_metadata : str
		Path to the csv file containing the sessions metadata
	"""

	def __init__(self, path_speeches, path_metadata, sep=";"):
		self.path_speeches = path_speeches
		self.path_metadata = path_metadata
		self.sep = sep
	
	def process_csv(self, save_csv=True, download_legi_csv=True, path_legi_csv="deputati"):
		"""
		Full processing of the two csv.

		Parameters
		----------
		save_csv : bool (default=True)
			Whether to write the resulting DataFrame to a csv file.
		
		download_legi_csv : bool (default=True)
			Whether we need to download deputies data from dati.camera.it
			because we have never done it (i.e.: we do not have several
			csv files containing info about the deputies in each legislature)

		path_legi_csv : str (default='deputati')
			The path where we will save (or load) the csv files with info
			download from dati.camera.it

		Returns
		-------
		df : Pandas DataFrame
		"""
		
		self.df = pd.read_csv(self.path_speeches, sep=self.sep)

		self.df = self.df.apply(self.get_session_text, axis=1)

		self.df = self.df.loc[self.df["president"] != ""]
		self.df = self.df.loc[self.df["cleaned"] != True]

		speeches = [self.get_speech(row) for _, row in self.df.iterrows()]

		self.df = pd.DataFrame(speeches, columns=['convocationid', 'deputy', 'text'])

		self.df = self.df[self.df.deputy != "PRESIDENTE"]
		self.df = self.df.dropna(subset=["text"])

		self.load_nltk_stopwords()

		self.df.text = self.df.text.apply(self.cleanText)
		self.df = self.df.drop(self.df.loc[self.df['text']==""].index)

		dc = DatiCamera(path_legi_csv)

		if download_legi_csv:
			dc.download_legi_csv()
		
		self.legi_df = dc.merge_legi_csv()
		self.deputy_df = dc.create_deputy_list()

		self.df["gender"] = self.df.apply(self.get_dep_sex, axis=1)

		# Remove speeches of which we couldn't find the gender of the deputy
		self.df.drop(self.df.loc[self.df["gender"]==""].index, inplace=True)

		metadata = pd.read_csv(self.path_metadata, sep=self.sep)
		metadata["date"] = metadata['date'].astype('datetime64[s]')
		metadata = metadata[["id", "date"]]

		self.df = self.df.merge(metadata, left_on='convocationid', right_on='id')

		self.create_legi_list()

		self.df["legislature"] = self.df.date.apply(self.get_legi)

		self.df = self.df[["convocationid", "deputy", "text", "gender", "date", "legislature"]]

		self.df["party"] = self.df.apply(self.get_dep_party, axis=1)
		self.df = self.df.dropna(subset=["party"])

		if save_csv:
			self.df.to_csv("discorsi_cleaned_ext.csv", index=False)
		
		return self.df

	def get_dep_party(self, x):
		"""
		Retrieve the deputy.

		Parameters
		----------
		x : pandas Series
			A row of a dataframe

		Returns
		-------
		A str with the party name

		"""
		idxs = self.legi_df.loc[self.legi_df.surname_name.isin([x.deputy])].index.tolist()
		
		if (len(idxs) > 0):
			if (len(idxs) == 1):
				return self.legi_df.iloc[idxs[0]].party
			
			df_temp = self.legi_df.iloc[idxs]
			df_temp = df_temp.reset_index(drop=True)
			for i in range(df_temp.shape[0]):
				range_date = pd.date_range(start=df_temp.iloc[i].start,end=df_temp.iloc[i].end).to_pydatetime().tolist()
				if x.date in range_date:
					return df_temp.iloc[i].party
		else:
			idxs = self.legi_df.loc[self.legi_df.name_surname.isin([x.deputy])].index.tolist()
			if (len(idxs) > 0):
				if (len(idxs) == 1):
					return self.legi_df.iloc[idxs[0]].party
				
				df_temp = self.legi_df.iloc[idxs]
				df_temp = df_temp.reset_index(drop=True)
				for i in range(df_temp.shape[0]):
					range_date = pd.date_range(start=df_temp.iloc[i].start,end=df_temp.iloc[i].end).to_pydatetime().tolist()
					if x.date in range_date:
						return df_temp.iloc[i].party
			else:
				idxs = self.legi_df.loc[self.legi_df.cognome.isin([x.deputy])].index.tolist()
				if (len(idxs) > 0):
					if (len(idxs) == 1):
						return self.legi_df.iloc[idxs[0]].party
					
					df_temp = self.legi_df.iloc[idxs]
					df_temp = df_temp.reset_index(drop=True)
					for i in range(df_temp.shape[0]):
						range_date = pd.date_range(start=df_temp.iloc[i].start,end=df_temp.iloc[i].end).to_pydatetime().tolist()
						if x.date in range_date:
							return df_temp.iloc[i].party
				else:
					return ""
	
	def load_nltk_stopwords(self, lang='italian'):
		"""
		Retreive stopwords and punctuation from nltk package and store them in a list.

		Parameters
		----------
		lang : str (default='italian')
			The language of text.
		"""
		self.stopwords = nltk.corpus.stopwords.words('italian')
		self.stopwords.extend(string.punctuation)
	
	def cleanText(x):
		"""
		Clean and tokenize the text.

		Parameters
		----------
		x : str
			Word or sentence or text

		Returns
		-------
		txt : str
		"""
		txt = str(x).lower()
		tokens = nltk.word_tokenize(txt)
		words = [word for word in tokens if word.isalpha()]
		table = str.maketrans('', '', string.punctuation)
		stripped = [w.translate(table) for w in tokens]
		words = [w for w in stripped if w.isalpha() and not w in stopwords]
		txt = " ".join(words)
		
		txt = re.sub(' +', ' ', txt)
		
		return txt

	def get_legi(self, x):
		"""
		Recognize the legislature to which each speech belongs to.

		Parameters
		----------
		x : str

		Returns
		-------
		i : int
		"""
		data = x.to_pydatetime()
		found = False
		i = 0
		while (found == False):
			leg = self.legis[i]

			if data in leg:
				found = True
				break

			i += 1
		
		return i + 1
		
	def get_dep_sex(self, x):
		"""
		Retrieve the sex of each deputy.

		Parameters
		----------
		x : pandas Series

		Returns
		-------
		genere : str
		"""
		idxs = self.deputy_df.loc[self.deputy_df.surname_name.isin([x.deputy])].index.tolist()
		if (len(idxs) > 0):
			return self.deputy_df.iloc[idxs[0]].genere
		else:
			idxs = self.deputy_df.loc[self.deputy_df.name_surname.isin([x.deputy])].index.tolist()
			if (len(idxs) > 0):
				return self.deputy_df.iloc[idxs[0]].genere
			else:
				idxs = self.deputy_df.loc[self.deputy_df.cognome.isin([x.deputy])].index.tolist()
				if (len(idxs) > 0):
					return self.deputy_df.iloc[idxs[0]].genere
				else:
					return ""
	

	def get_president_name(self, x):
		"""
		Retrieve the name of each president per each session.

		Parameters
		----------
		x : pandas Series

		Returns
		-------
		splt = list
		"""
		idx = x.find("PRESIDENZ")
		pres = x[idx+11:]
		idx = pres.find("\n")
		pres = pres[:idx]
		splt = pres.split()
		
		if len(splt) > 0:
			return splt[-1]
		else:
			return ""
	
	def isRomanNumber(self, x):
		"""
		Retrieve roman numbers in the text.

		Parameters
		----------
		x : any

		Returns
		-------
		boolean
		"""
		match = re.match(r"^(?=[MDCLXVI])M*(C[MD]|D?C{0,3})(X[CL]|L?X{0,3})(I[XV]|V?I{0,3})$", x)
		
		if match is not None:
			return True
		else:
			return False
	
	def get_session_text(self, x):
		"""
		Split the text per sessions.

		Parameters
		----------
		x : pandas Series

		Returns
		-------
		A pandas Series
		"""
		col = ['id', 'convocationid', 'downloadtime', 'text', 'session_text', 'president', 'cleaned']
		splitted = x.text.split("La seduta comincia")
		
		presidente = self.get_president_name(x.text)
		
		if len(splitted) == 2:
			testo = splitted[1]
			pos = testo.find("\n")
			
			end = -1
			if 'PAGINA BIANCA' in testo:
				end = testo.find('PAGINA BIANCA')
			if end != -1:
				return pd.Series([x.id, x.convocationid, x.downloadtime, x.text, testo[pos + 1:end], presidente, True], index=col)
			else:
				return pd.Series([x.id, x.convocationid, x.downloadtime, x.text, testo[pos + 1:], presidente, True], index=col)
		else:
			return pd.Series([x.id, x.convocationid, x.downloadtime, x.text, x.text, presidente, False], index=col)
	
	def get_speech(self, x):
		"""
		Divide each single intervention.

		Parameters
		----------
		x : A pandas Series

		Returns
		-------
		speeches : list
		"""
		speeches = []
		
		pattern = r"(?:\n)+(\b[A-Z ]+\b)"
		l = re.finditer(pattern, x.session_text)
		custom_stopwords = ['LEGISLATURA', 'PAGINA BIANCA',
                        'STENOGRAFICO', 'STENOGRAFIA',
                        'TIPOGRAFIA', 'DISCUSSIONI',
                        'SEDUTA', 'VOTAZIONI', 'DISCUS']
		
		results = []
		for match in l:
			name = str(match.group(0)).strip()
			
			if name != '\\n' and (len(name) > 1) and not any(w in name for w in custom_stopwords) and not isRomanNumber(name):
				results.append([name, match.start(), match.end()])
				
		toRemove = []
		for i, item in enumerate(results):
			if i+1 < len(results):
				if item[2] == results[i+1][1]:
					if i not in toRemove:
						toRemove.append(i)
					if i+1 not in toRemove:
						toRemove.append(i+1)
						
		[results.pop(i) for i in sorted(toRemove, reverse=True)]
		
		for i, item in enumerate(results):
			if i+1 < len(results):
				speech = x.session_text[item[2]:results[i+1][1]]
			elif i+1 == len(results):
				speech = x.session_text[item[2]:]
			else:
				raise ValueError

			#dep = x.presidente if item[0] == "PRESIDENTE" else item[0]
			dep = item[0]
			
			speeches.append([x.convocationid, dep, speech.strip()])
			
		return speeches

	def create_legi_list(self):
		"""
		Create the list with the legislatures and assign the correspondent speech to each.

		Parameters
		----------
		self
		"""

		leg01 = pd.date_range(start="1948-05-08",end="1953-06-24").to_pydatetime().tolist()
		leg02 = pd.date_range(start="1953-06-25",end="1958-06-11").to_pydatetime().tolist()
		leg03 = pd.date_range(start="1958-06-12",end="1963-05-15").to_pydatetime().tolist()
		leg04 = pd.date_range(start="1963-05-16",end="1968-06-04").to_pydatetime().tolist()
		leg05 = pd.date_range(start="1968-06-05",end="1972-05-24").to_pydatetime().tolist()
		leg06 = pd.date_range(start="1972-05-25",end="1976-07-04").to_pydatetime().tolist()
		leg07 = pd.date_range(start="1976-07-05",end="1979-06-19").to_pydatetime().tolist()
		leg08 = pd.date_range(start="1979-06-20",end="1983-07-11").to_pydatetime().tolist()
		leg09 = pd.date_range(start="1983-07-12",end="1987-07-01").to_pydatetime().tolist()
		leg10 = pd.date_range(start="1987-07-02",end="1992-04-22").to_pydatetime().tolist()
		leg11 = pd.date_range(start="1992-04-23",end="1994-04-14").to_pydatetime().tolist()
		leg12 = pd.date_range(start="1994-04-15",end="1996-05-08").to_pydatetime().tolist()
		leg13 = pd.date_range(start="1996-05-09",end="2001-05-29").to_pydatetime().tolist()
		leg14 = pd.date_range(start="2001-05-30",end="2006-04-27").to_pydatetime().tolist()
		leg15 = pd.date_range(start="2006-04-28",end="2008-04-28").to_pydatetime().tolist()
		leg16 = pd.date_range(start="2008-04-29",end="2013-03-14").to_pydatetime().tolist()
		leg17 = pd.date_range(start="2013-03-15",end="2018-03-22").to_pydatetime().tolist()
		leg18 = pd.date_range(start="2018-03-23",end=datetime.strftime(datetime.today(), '%Y-%m-%d')).to_pydatetime().tolist()

		self.legis = [leg01, leg02, leg03, leg04,
		 leg05, leg06, leg07,
		  leg08, leg09, leg10,
		   leg11, leg12, leg13,
		    leg14, leg15, leg16,
			 leg17, leg18]