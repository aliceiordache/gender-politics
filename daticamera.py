from datetime import datetime
import pandas as pd
import glob, os

class DatiCamera():

	def __init__(self, path="deputati"):
		self.path = path

	def download_anagraf(self, save_csv=True):
		"""
		Download the file containing all info about deputies for the legislatures considered in the dataset.

		Parameters
		----------
		save_csv : bool (default=True)
			Whether to write the resulting DataFrame to a csv file.
		"""
		legis = list(range(1, 19))

		for legi in legis:
			df_dep = pd.read_csv(r"http://dati.camera.it/sparql?query=%23%23%23%23+tutti+i+deputati+nella+XVI+Legislatura+con+info%2C+estremi+di+mandato+e+numero+totale+di+mandati+%0D%0A%23%23%23%23+%28anche+successivi%29+%0D%0A%23%23%23%23+%28la+URI+%3Chttp%3A%2F%2Fdati.camera.it%2Focd%2Flegislatura.rdf%2Frepubblica_16%3E+identifica+la+Legislatura%29%0D%0A%0D%0ASELECT+DISTINCT+%3Fpersona+%3Fcognome+%3Fnome+%3Finfo%0D%0A%3FdataNascita+%3FluogoNascita+%3Fgenere+%3FnomeGruppo+%3FinizioMandato+%3FfineMandato%0D%0A%3Fcollegio++COUNT%28DISTINCT+%3FmadatoCamera%29+as+%3FnumeroMandati+%3Faggiornamento+%0D%0AWHERE+%7B%0D%0A%3Fpersona+ocd%3Arif_mandatoCamera+%3Fmandato%3B+a+foaf%3APerson.%0D%0A%0D%0A%23%23+deputato%0D%0A%3Fd+a+ocd%3Adeputato%3B+ocd%3Aaderisce+%3Faderisce%3B%0D%0Aocd%3Arif_leg+%3Chttp%3A%2F%2Fdati.camera.it%2Focd%2Flegislatura.rdf%2Frepubblica_"+ "{:02d}".format(legi) + r"%3E%3B%0D%0Aocd%3Arif_mandatoCamera+%3Fmandato.%0D%0AOPTIONAL%7B%3Fd+dc%3Adescription+%3Finfo%7D%0D%0A%0D%0A%23%23anagrafica%0D%0A%3Fd+foaf%3Asurname+%3Fcognome%3B+foaf%3Agender+%3Fgenere%3Bfoaf%3AfirstName+%3Fnome.%0D%0AOPTIONAL%7B%0D%0A%3Fpersona+%3Chttp%3A%2F%2Fpurl.org%2Fvocab%2Fbio%2F0.1%2FBirth%3E+%3Fnascita.%0D%0A%3Fnascita+%3Chttp%3A%2F%2Fpurl.org%2Fvocab%2Fbio%2F0.1%2Fdate%3E+%3FdataNascita%3B+%0D%0Ardfs%3Alabel+%3Fnato%3B+ocd%3Arif_luogo+%3FluogoNascitaUri.+%0D%0A%3FluogoNascitaUri+dc%3Atitle+%3FluogoNascita.+%0D%0A%7D%0D%0A%23%23aggiornamento+del+sistema%0D%0AOPTIONAL%7B%3Fd+%3Chttp%3A%2F%2Flod.xdams.org%2Fontologies%2Fods%2Fmodified%3E+%3Faggiornamento.%7D%0D%0A%23%23+mandato%0D%0A%3Fmandato+ocd%3Arif_elezione+%3Felezione.++%0D%0AOPTIONAL%7B%3Fmandato+ocd%3AendDate+%3FfineMandato.%7D%0D%0AOPTIONAL%7B%3Fmandato+ocd%3AstartDate+%3FinizioMandato.%7D%0D%0A+%0D%0A%23%23+totale+mandati%0D%0A%3Fpersona+ocd%3Arif_mandatoCamera+%3FmadatoCamera.%0D%0A+%0D%0A%23%23+elezione%0D%0A%3Felezione+dc%3Acoverage+%3Fcollegio.%0D%0A++%0D%0A%23%23+adesione+a+gruppo%0D%0AOPTIONAL%7B%0D%0A++%3Faderisce+ocd%3Arif_gruppoParlamentare+%3Fgruppo.%0D%0A++%3Fgruppo+%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2Falternative%3E+%3Fsigla.%0D%0A++%3Fgruppo+dc%3Atitle+%3FnomeGruppo.%0D%0A%7D%0D%0A+%0D%0A%7D&debug=on&default-graph-uri=&format=text%2Fcsv")
			
			df_dep["legislatura"] = legi
			df_dep.to_csv(self.path + "/legi" + "{:02d}".format(legi) + ".csv", index=False)
	
	def _get_csv_list(self):
		"""
		Retrieve the list from the csv file.

		Parameters
		----------
		self

		Returns
		-------
		files : list
		"""
		files = glob.glob(self.path + '/legi*')
		files.sort()

		return files
	
	def merge_legis_csv(self):
		"""
		Merge the csv with info from official website with our dataset.

		Parameters
		----------
		self

		Returns
		-------
		df : Pandas DataFrame
		"""
		files = self._get_csv_list()

		df = pd.concat(map(lambda file: pd.read_csv(file), files))
		df = df.reset_index(drop=True)

		df["name_surname"] = df.apply(lambda x: x.nome + " " + x.cognome, axis=1)
		df["surname_name"] = df.apply(lambda x: x.cognome + " " + x.nome, axis=1)
		df["party"] = df.nomeGruppo.apply(lambda x: x.split("(")[0].strip())
		df["acronym"] = df.nomeGruppo.apply(lambda x: x.split("(")[1].replace(")","").strip())
		df["start"] = df.nomeGruppo.apply(lambda x: x.split("(")[2].split("-")[0])
		df["end"] = df.nomeGruppo.progress_apply(lambda x: x.split("(")[2].split("-")[1].replace(")", "") if "-" in x.split("(")[2] else datetime.strftime(datetime.today(), '%d.%m.%Y'))

		to_rem = [self.try_strptime(df.iloc[i].start) for i in range(df.shape[0])]
		to_rem = [i for i in to_rem if i is not None]
		df.drop(df.index[to_rem], inplace=True)
		df = df.reset_index(drop=True)

		return df
	
	def try_strptime(self, i):
		"""
		Remove dates not compatible with our dataset.

		Parameters
		----------
		s : str
			The date

		Returns
		-------
		s : str
		"""
		try:
			date = datetime.strptime(s, "%d.%m.%Y")
		except ValueError:
			return s
		return None
	
	def create_deputy_list(self):
		"""
		Create the list with all deputies considered.

		Parameters
		----------
		self

		Returns
		-------
		df : Pandas DataFrame
		"""
		files = self._get_csv_list()

		df = pd.concat(map(lambda file: pd.read_csv(file, usecols=['cognome', 'nome', 'genere']), files))
		df = df.drop_duplicates()
		df = df.reset_index()

		# We create new columns in order to get gender (we need the combinations of name and surname)
		# Deputies are reported in different ways in speeches, we need to consider all variations
		df["name_surname"] = df.apply(lambda x: x.nome + " " + x.cognome, axis=1)
		df["surname_name"] = df.apply(lambda x: x.cognome + " " + x.nome, axis=1)

		return df