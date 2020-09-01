# *Daten vorbereiten mit Python*

Unter Begriffen wie Datapreparation, Datashaping oder Datatransformation wird die Aufbereitung unstrukturierter Daten beim ETL-Prozess verstanden, um sie für eine nachfolgende Analyse vorzubereiten. Das Vorbereiten von Daten entspricht dabei dem T im ETL-Prozess.
In diesem Kapitel beschäftigen wir uns mit dem Vorbereiten von Daten, das hauptsächlich aus zwei Einzelschritten besteht:

* Daten bereinigen bzw. Datacleansing
* Daten transformieren bzw. Datashaping
## Daten bereinigen
Unterschiedliche Datentypen in einer Spalte, unterschiedliche Schreibweisen einzelner Werte oder einfache Schreibfehler wie führende oder folgende Leerzeichen verhindern eine strukturierte Auswertung von Daten. Beim Bereinigen von Daten sollten folgende Grundsätze beachtet werden:
* Statistische Auswertungen oder weitergehende ML-basierte Modelle können besser mit numerischen Werten als mit Text arbeiten. Wenn nicht gerade eine Textklassifikation angestrebt wird, sollten Daten möglichst in rein numerische Werte (int, float o.ä.) übersetzt werden.

* Python ist Case-sensitive und würde damit die Wörter "Haus" und "haus" als zwei unterschiedliche Wörter ansehen. Das ist insbesondere hinderlich beim Zählen und Kategorisieren von Daten. Es ist für die Auswertung hilfreich, Textinformationen komplett in Groß -oder Kleinschreibung zu übersetzen.
* Viele Analysemethoden reagieren empfindlich auf fehlende Daten. Liefern einzelne Daten einen Null-Wert, sollte überlegt werden, diesen Datensatz komplett zu löschen oder zu maskieren.
***
Für die nächsten Übungen bauen wir folgendes einfaches Dataframe auf:

```python
import pandas as pd 

dict={"Name":pd.Series([" Hans-Peter","Karl-Heinz","Petra"," Julia	"]),
    "Beruf":pd.Series(["Beamter","Kaufmann","Lehrerin","Musikerin"]),
    "Alter":pd.Series([35,42,50,61])}

df=pd.DataFrame(dict)
```

```
          Name      Beruf  Alter
0   Hans-Peter    Beamter     35
1   Karl-Heinz   Kaufmann     42
2        Petra   Lehrerin     50
3       Julia   Musikerin     61
```

***

**Trimmen und Kürzen von Texten**<br>
In dem Dataframe sind einige überschüssige Leer -und Sonderzeichen versteckt, was eine Auswertung erschwert. Zuerst finden wir heraus, wo unser Dataframe in der Spalte "Name" davon betroffen ist.

```python
print (df.loc[df.Name.str.match(r".*\s$") | df.Name.str.match(r"^\s.*")])
```

```
          Name      Beruf  Alter
0   Hans-Peter    Beamter     35
3      Julia    Musikerin     61
```

Über die Zeichenklasse \s wird auch der Tabulator gefunden, der sich hinter Julia versteckt. Mittels der Stringfunktion strip() können die überschüssigen Leerzeichen nun entfernt werden.

```python
df["Name"]=df["Name"].str.strip()
```

Das erspart bei Tabellen mit einer sechsstelligen Anzahl an Zeilen einige Sucharbeit. Warum aber nun diese Funktion nicht generell über alle Spalten laufen lassen? Versuchen wir es mal:

```python
for column in df.columns:
    df[column]=df[column].str.strip()
```

```
AttributeError: Can only use .str accessor with string values!
```

Ok, irgendeine Spalte verwendet als Datentyp keine Strings. Der Verdacht würde ja auf die Spalte "Alter" fallen. Schauen wir mal:

```python
print (df.info())
```

```
 #   Column  Non-Null Count  Dtype
---  ------  --------------  -----
 0   Name    4 non-null      object
 1   Beruf   4 non-null      object
 2   Alter   4 non-null      int64
dtypes: int64(1), object(2)
```

Der Verdacht hat sich bestätigt, Stringfunktionen können natürlich nicht auf Integerwerte angewendet werden. Es bleibt also , entweder nur verdächtige Spalten zu durchsuchen und zu trimmen oder bei sehr vielen Spalten unsere Schleife auszubauen:

```python
for column in df.columns:
    if df[column].dtype==object:
        df[column]=df[column].str.strip()
```

***

**Indizieren von Datenstrukturen**<br>

In Pandas-Datenstrukturen gibt es zwei primäre Methoden zur Indizierung. .loc funktioniert labelbasiert und .iloc indexbasiert. Was heißt das jetzt konkret?

```python
#Wir lassen uns die erste Zeile ausgeben. Die 0 stellt dabei das Label der Indexspalte dar, nicht den Index der Einträge!
print (df.loc[0])

Name      Hans-Peter
Beruf        Beamter
Alter             35

#Wenn wir den Index nun auf die Namensspalte setzen, funktioniert die Label-indizierung mit 0 nicht mehr, da der Index der erste Zeile nun "Hans-Peter" heisst.
df=df.set_index("Name")
print (df.loc[0])
TypeError: cannot do label indexing on <class 'pandas.core.indexes.base.Index'> with these indexers [0] of <class 'int'>

# Wir müssen also entweder mit .loc das Label konkret benennen ("Hans-Peter"), oder mit .iloc die integerbasierte Indizierung nutzen (da wären wir wieder bei 0)
print (df.loc["Hans-Peter"])
print (df.iloc[0])
```

Das Verständnis für die unterschiedliche Arbeitsweise von .loc und .iloc ist wichtig für ein fehlerfreies Arbeiten. Generell kann man aber sagen, dass die Nutzung von .loc nur über die Indexspalte nicht empfehlenswert ist.<br>

Bis jetzt haben wir nur einzelne Zeilen identifiziert. Wir wollen aber ja auf Zellen innerhalb einer Zeile zugreifen. Auch dafür bieten .loc und .iloc ausreichend Möglichkeiten.

```python
#Das entspricht der ersten Zeile und der ersten Spalte nach dem Index
print (df.loc["Hans-Peter","Beruf"])

Beamter

#Dasselbe Ergebnis erzielen wir mit folgendem .iloc-Eintrag
print (df.iloc[0,0])
```

Um nun über alle Zeilen zu iterieren und mit den Daten der einzelnen Spalten zu arbeiten, gibt es mehrere Möglichkeiten.

```python
#nicht empfehlenswert. Daten sollten nicht inline bearbeitet werden, da python in einigen Fällen mit Kopien arbeitet
for rows in df.iterrows():
    print (rows)

#stattdessen...
#Gibt das gesamte Dataframe aus
print (df.loc[:,])
#gibt nur die Namensspalte aus
print (df.loc[:,"Name"])
#und nur den Beruf der ersten beiden Einträge
print (df.loc[0:1,"Beruf"])

#Und wer nicht auf die For-Schleife verzichten möchte, kann folgendes probieren
for zeile in range (0,len(df)):
    print(df.iloc[zeile,0],end=",")
    print (df.iloc[zeile,1],end=",")
    print (df.iloc[zeile,2])
    
Hans-Peter,Beamter,35
Karl-Heinz,Kaufmann,42
Petra,Lehrerin,50
Julia,Musikerin,61
```

Fortgeschrittene Suchfunktionen lassen sich am besten mit boolean-indexing umsetzen.

```python
#Wer in dem Dataframe ist Beamter?
print(df[df["Beruf"]=="Beamter"])

         Name    Beruf  Alter
0  Hans-Peter  Beamter     35

#und jetzt wollen wir nur den Namen des Beamten
print(df[df["Beruf"]=="Beamter"].Name)

0    Hans-Peter
Name: Name, dtype: object
        
#Wer ist älter als 40 und jünger als 60?
print(df[(df.Alter > 40) & (df.Alter <60)])

         Name     Beruf  Alter
1  Karl-Heinz  Kaufmann     42
2       Petra  Lehrerin     50

#Doppelnamen sollen ohne Bindestriche geschrieben werden
df.loc[df.Name.str.contains("-"),"Name"]=df.Name.str.replace("-"," ")

         Name      Beruf  Alter
0  Hans Peter    Beamter     35
1  Karl Heinz   Kaufmann     42

#Und die Berufsbezeichnung soll noch etwas genauer sein
df.loc[df["Beruf"]=="Beamter","Beruf"]="Verwaltungsbeamter"

         Name               Beruf  Alter
0  Hans-Peter  Verwaltungsbeamter     35
```

Es ist Geschmackssache, ob man dabei über Listcomprehensions schneller ans Ziel kommt:

```python
df["Geburtsjahr"]=[datetime.date.today().year-alter for alter in df["Alter"]]

         Name      Beruf  Alter  Geburtsjahr
0  Hans-Peter    Beamter     35         1985
1  Karl-Heinz   Kaufmann     42         1978
2       Petra   Lehrerin     50         1970
3       Julia  Musikerin     61         1959


#Statt des Doppelnamens soll nur der erste Namensbestandteil angezeigt werden
df["Name"]=[name.split("-")[0] for name in df["Name"]]

    Name      Beruf  Alter
0   Hans    Beamter     35
1   Karl   Kaufmann     42
2  Petra   Lehrerin     50
3  Julia  Musikerin     61

#Nun sollen die Namen groß geschrieben werden, um die spätere Auswertung zu erleichtern
df["Name"]=[name.upper() for name in df["Name"]]

         Name      Beruf  Alter
0  HANS-PETER    Beamter     35
1  KARL-HEINZ   Kaufmann     42
2       PETRA   Lehrerin     50
3       JULIA  Musikerin     61

#Und es wird noch die Branche als zusätzliche Spalte hinzugefügt
df["Branche"]=["Öffentlicher Dienst" if (beruf=="Beamter" or beruf=="Lehrerin") else "Freie Wirtschaft" for beruf in df["Beruf"]]

         Name      Beruf  Alter              Branche
0  Hans-Peter    Beamter     35  Öffentlicher Dienst
1  Karl-Heinz   Kaufmann     42     Freie Wirtschaft
2       Petra   Lehrerin     50  Öffentlicher Dienst
3       Julia  Musikerin     61     Freie Wirtschaft

#Natürlich lässt sich die bestehende Spalte aus inline ändern
df["Beruf"]=["Öffentlicher Dienst" if (beruf=="Beamter" or beruf=="Lehrerin") else "Freie Wirtschaft" for beruf in df["Beruf"]]

         Name                Beruf  Alter
0  Hans-Peter  Öffentlicher Dienst     35
1  Karl-Heinz     Freie Wirtschaft     42
2       Petra  Öffentlicher Dienst     50
3       Julia     Freie Wirtschaft     61
```

***

**Umgang mit Duplikaten**<br>

Duplikate in Datensätzen lassen sich nicht immer vermeiden und das ist häufig auch gar nicht möglich. In einem großen Datensatz mit Namen kommt es zwangsläufig vor, dass mehrere Personen denselben Namen nutzen. Problematisch wird die Situation, wenn die betroffenen Daten eine besondere Funktion in dem Datensatz einnehmen, z.B. als Index oder als Schlüsselspalte für join/merge-Operationen. Zudem benötigen mehr Daten auch mehr Speicher und mehr CPU-Zyklen. Es ist also immer eine Frage des Einzelfalles, ob das Löschen von Duplikaten unerwünschte Nebeneffekte hätte. In den seltensten Fällen macht aber ein generelles Löschen von Duplikaten über das gesamte Dataframe Sinn.<br>

Wir nutzen wieder das Dataframe aus dem vorherigem Kapitel, bauen es aber ein wenig um, um die Sinnhaftigkeit beim Umgang mit Duplikaten darzustellen.

```python
dict={"Name":pd.Series(["Hans-Peter","Karl-Heinz","Petra","Julia","Julia"]),
    "Beruf":pd.Series(["Beamter","Beamter","Lehrerin","Musikerin","Musikerin"]),
    "Alter":pd.Series([35,42,42,61,61])}

df=pd.DataFrame(dict)

         Name      Beruf  Alter
0  Hans-Peter    Beamter     35
1  Karl-Heinz    Beamter     42
2       Petra   Lehrerin     42
3       Julia  Musikerin     61
4       Julia  Musikerin     61
```

Hans-Peter und Karl-Heinz haben nun zufällig denselben Beruf und Karl-Heinz und Petra sind zufällig gleich alt. Dafür wollen wir sie natürlich nicht bestrafen und wir müssen schauen, wie wir mit dieser Art von Duplikaten im weiteren vorgehen. Auffällig sind die letzten beiden Einträgen mit dem Index 3 und 4. Diese stimmen in allen drei Spalten überein, ist also ein Duplikat im klassischen Sinne.<br>

Wie finden wir nun heraus, ob in unserem Datensatz Duplikate vorkommen?

```python
print(df.duplicated())

0    False
1    False
2    False
3    False
4     True
```

Das entspricht schon ungefähr dem von uns erwartetem Ergebnis. Der letzte Eintrag mit dem Index 4 ist identisch mit dem Index 3. Allerdings kommt es nicht zwangsläufig vor, dass doppelte Einträge untereinander stehen. Wie kriegen wir nun heraus, zu welchem zweiten (und eventuell dritten, vierten...) Duplikat der Eintrag gehört?

```python
print(df.duplicated(keep=False))

0    False
1    False
2    False
3     True
4     True
```

Mit dem Parameter keep wird festgelegt, welcher der gefundenen Duplikate auf True gesetzt wird. Standardmäßig werden mit keep=first alle außer dem ersten Duplikat auf True gesetzt. Mit keep=last wird nur das letzte Duplikat nicht angezeigt (also mit False markiert). Mit keep=False werden alle Duplikate auf True gesetzt. Um zu schauen, welcher Eintrag nun wirklich davon betroffen ist, können wir uns wieder mit boolean indexing behelfen.

```python
print (df.loc[df.duplicated()])
    Name      Beruf  Alter
4  Julia  Musikerin     61

#Mit dem Parameter keep=False sehen wir auch, wie oft das Duplikat vorkommt
print (df.loc[df.duplicated(keep=False)])

3  Julia  Musikerin     61
4  Julia  Musikerin     61
```

Wenn wir aber auch Duplikate in anderen Spalten finden wollen, können wir die Suche auf einzelne Spalten reduzieren.

```python
print(df.duplicated(subset="Beruf",keep=False))

0     True
1     True
2    False
3     True
4     True
```

Das passt! Wir haben zwei Beamte und zwei Musikerinnen.

Wir wollen nun die Duplikate löschen. Im  einfachsten Fall löschen wir nur den doppelten Eintrag mit dem Index 3 und 4.

```python
print(df.drop_duplicates())

         Name      Beruf  Alter
0  Hans-Peter    Beamter     35
1  Karl-Heinz    Beamter     42
2       Petra   Lehrerin     42
3       Julia  Musikerin     61
```

Damit hätten wir paretomäßig schon mal 80% der Fälle abgedeckt. Schauen wir aber auch nochmal auf die verbleibenden 20%.

```python
#Wir müssen die Anzahl an Beamten verringern!
print(df.drop_duplicates(subset="Beruf"))

         Name      Beruf  Alter
0  Hans-Peter    Beamter     35
2       Petra   Lehrerin     42
3       Julia  Musikerin     61

#oder die Diversität in der Alterstrukur erhöhen
print(df.drop_duplicates(subset="Alter"))

0  Hans-Peter    Beamter     35
1  Karl-Heinz    Beamter     42
3       Julia  Musikerin     61

#Wenn Duplikate unglaubwürdig wirken, kann man auch alle betroffenen Einträge löschen
print(df.drop_duplicates(keep=False))

         Name     Beruf  Alter
0  Hans-Peter   Beamter     35
1  Karl-Heinz   Beamter     42
2       Petra  Lehrerin     42
```

***
**Umgang mit NAN-Werten**<br>
Ein Ansatz zum Umgang mit fehlenden Werten ist die Verwendung des Python Objektes None, der in allen Datenstrukturen verwendet werden kann. Nachteil dieses Vorgehens ist, dass damit die betroffene Datenstruktur automatisch den dtype=object erhält und numerische Operationen wie min, max, sum auf das Array mit einem Fehler beendet werden.

```python
import numpy as np
fibonacci = np.array([1,1, None,3,5,8,13,21,34])
fibonacci

array([1, 1, None, 3, 5, 8, 13, 21, 34], dtype=object)
```


Besteht eine Datenstruktur aus rein numerischen Daten, sollte daher lieber NaN (not a Number) verwendet werden, der einen speziellen Float-Wert darstellt.
```python
import numpy as np
fibonacci = np.array([1,np.nan,2,3,5,8,13,21,34])

array([ 1., nan,  2.,  3.,  5.,  8., 13., 21., 34.])
```
Der dtype wird damit automatisch von int zu float64. Zu beachten ist dabei allerdings, dass damit zwar numerische Operationen ohne Fehler durchgeführt werden, aber nicht immer das erwartete Ergebnis produzieren.
```python
fibonacci.sum()

nan
```

Bis jetzt haben wir das Problem von allen Seiten umkreist und bewundert. Was machen wir aber nun mit Null-Werten in Datensätzen? Zum Glück bietet uns Pandas zumindest bei den Series und Dataframes umfangreiche Möglichkeiten, mit Nullwerten zu arbeiten.

Machen wir also aus unserem Array zunächst eine Series.

```python
fibonacci = np.array([1,1, None,3,5,8,13,21,34])
fibonacci_series=pd.Series(fibonacci)

0       1
1       1
2    None
3       3
4       5
5       8
6      13
7      21
8      34
```

Die Ausgabe ist ähnlich wie bei dem Array, wir haben zusätzlich allerdings noch einen Index bekommen. Über die beiden Funktionen isnull() können wir nun einfach herausbekommen, welche Datensätze fehlerhaft sind. Mit der Funktion notnull() würde das Ergebnis umgedreht werden.

```python
print (fibonacci_series.isnull())

0    False
1    False
2     True
3    False
4    False
5    False
6    False
7    False
8    False
```

Über boolean-Indexing können wir nun auch auf die Zeilen filtern, die NaN-Werte enthalten.

```python
print(fibonacci_series[fibonacci_series.isnull()]==True)

2    False
dtype: bool
```

Wir können jetzt händisch die fehlerhaften Werte mit einer von uns gewählten Maskierung füllen.

```python
fibonacci_series.loc[fibonacci_series.isnull()]=-1

0     1
1     1
2    -1
3     3
4     5
5     8
6    13
7    21
8    34
```

Einfacher und weniger fehleranfällig sind allerdings die Standardfunktionen der Pandasbibliothek

```python
#Löscht die komplette Zelle mit Nullwerten
print (fibonacci_series.dropna())

0     1
1     1
3     3
4     5
5     8
6    13
7    21
8    34

#Ergibt dasselbe Ergebnis wie bei der manuellen Bearbeitung
print (fibonacci_series.fillna(-1))

0     1
1     1
2    -1
3     3
4     5
5     8
6    13
7    21
8    34
```

Es hängt vom Einzelfall ab, ob man besser die gesamte Zeile löschen oder mit einem Füllwert maskieren soll. Folgende Aspekte müssen dabei beachtet werden:

- Wenn mit Maskierung gearbeitet wird, schränkt das den Raum möglicher Datenwerte ein. Zudem könnten Verzerrungen oder Fehler bei weitergehenden Analysen durch die Maskierung auftreten
- Wenn die Zeile komplett gelöscht wird, sind auch die Werte in weiteren Spalten verloren. Das ist insbesondere problematisch, wenn sich weitergehende Analysen insbesondere auf nicht betroffene Spalten beziehen.

Wenn also, wie in unserem Beispiel nur eine Spalte betroffen ist, ist zu empfehlen, die Zeile zu löschen. Bei Datensätzen mit vielen Spalten und NaN-Werten in für die weitere Verarbeitung unwichtigen Spalten sollten die Werte maskiert werden. Die NaN-Werte zu behalten, ist nicht zu empfehlen.

## Daten transformieren
Im vorherigen Kapitel haben wir die Daten inhaltlich so weit bereinigt, dass sie zumindest theoretisch auswertbar sind. In diesem Kapitel geht es nun darum, die Daten in eine Struktur zu bringen, damit eine automatische Auswertung überhaupt funktioniert. Datenstrukturen sind häufig nach optischen Gesichtspunkten aufgebaut, damit sie durch das menschliche Auge konsumiert werden können. Menschen gelingt dabei zwar automatisch der Kontextbezug, das gelingt aber nur bei einer geringen Datenmengen. Bei großen Datenbeständen spielen Automatismen ihre Vorteile aus. Allerdings müssen die Daten dafür in einem Format vorliegen, dass keine Interpretation erfordert. Der Fokus beim transformieren liegt auf folgenden Tätigkeiten:

- Daten in eine kontextfreie Struktur zu bringen
- Daten aus unterschiedlichen Blickwinkeln betrachten, um neue Informationen aus den Daten zu erhalten
- Daten aus mehreren Datenquellen zusammenzubringen, um die Informationsdichte zu erhöhen
- Daten zur Dimensionsreduktion aggregieren

Zuerst bauen wir uns über ein Dictionary ein einfaches Dataframe auf, mit dem wir im Laufe des Kapitels noch arbeiten werden.

```python
import pandas as pd 
dict={"A":pd.Series(["x","y","x","y"]),
    "B":pd.Series([1,2,1,2]),
    "C":pd.Series([3,4,5,6]),
    "D":pd.Series([10,20,30,40])}

df=pd.DataFrame(dict)
```
```
   A  B  C   D
0  x  1  3  10
1  y  2  4  20
2  x  1  5  30
3  y  2  6  40
```
**Spalten tauschen, kombinieren und trennen**<br>

Das Tauschen von Spalten ist relativ einfach, da die Spaltenreihenfolge als Tuple in der Funktion DataFrame.Columns gespeichert ist. Es reicht, die Reihenfolge der Spaltenüberschriften zu ändern.

```python
print(df[["Alter","Beruf","Name"]])
   Alter      Beruf        Name
0     35    Beamter  Hans-Peter
1     42   Kaufmann  Karl-Heinz
```

So ähnlich funktioniert auch das Kombinieren von Spalten. Zuerst teilen wir Doppelnamen auf unterschiedliche Spalten auf.

```python
df["Name"]=df["Name"]+" "+df["Beruf"]

                 Name      Beruf  Alter
0   Hans-Peter Beamter    Beamter     35
1  Karl-Heinz Kaufmann   Kaufmann     42
2       Petra Lehrerin   Lehrerin     50
3      Julia Musikerin  Musikerin     61
```

Da die Spalte "Beruf" noch vorhanden ist, liegen die Daten nun doppelt vor. Wir müssen also noch die Spalte löschen.

```python
#Wir müssen axis=1 setzen, da python standardmäßig nach dem Index (also der Zeile) sucht. Wir wollen aber ja die Spalte löschen
print(df.drop("Beruf",axis=1))

                  Name  Alter
0   Hans-Peter Beamter     35
1  Karl-Heinz Kaufmann     42
2       Petra Lehrerin     50
3      Julia Musikerin     61
```

Beim Trennen von Spalten funktioniert ebenfalls die standardmäßige Stringoperation .split(). Der Parameter expand=True ist notwendig, da python ansonsten die Liste mit den gesplitteten Werten in die Zelle schreibt. Zudem müssen wir python noch mitteilen, auf welche neuen Spalten die neuen Werte aufgeteilt werden sollen. Ansonsten würde nur der erste Wert in die alte Spalte übernommen werden.

```python
df[["1. Name","2. Name"]]=df["Name"].str.split("-",expand=True)
print (df.drop("Name",axis=1))
       Beruf  Alter 1. Name 2. Name
0    Beamter     35    Hans   Peter
1   Kaufmann     42    Karl   Heinz
2   Lehrerin     50   Petra    None
3  Musikerin     61   Julia    None
```

Und da wir weiter oben ja schon gelernt haben, dass None-Werte in Datensätzen vermieden werden sollten, maskieren wir die leeren Zellen mit einem beliebigen Füllwert.

```python
print (df.fillna("-"))
         Name      Beruf  Alter 1. Name 2. Name
0  Hans-Peter    Beamter     35    Hans   Peter
1  Karl-Heinz   Kaufmann     42    Karl   Heinz
2       Petra   Lehrerin     50   Petra       -
3       Julia  Musikerin     61   Julia       -
```

**Datenstrukturen transponieren**<br>

Im einfachsten Fall transponieren wir die Tabelle, indem wir sie um 90 Grad kippen. Aus den Spaltenüberschriften wird dadurch der Index und der vorherige Index zu den Spaltenüberschriften.
```python
df_transpose=df.transpose()
print(df_transpose)
```
```
    0   1   2   3
A   x   y   x   y
B   1   2   1   2
C   3   4   5   6
D  10  20  30  40
```

Wir laden beispielhaft die Klimatabelle für die Stadt Kiel:
```python
import pandas as pd 
import requests

url="https://www.wetterkontor.de/de/klima/klima2.asp?land=de&stat=10046"  
r=requests.get(url)
df_list=pd.read_html(r.text)
df=df_list[0]
print(df.head(3))
```
```
  Unnamed: 0  Temperatur °Cmax. Ømin. Ø  Temperatur °Cmax. Ømin. Ø.1  NiederschlagmmTage  NiederschlagmmTage.1  relativeFeuchte  Sonneh/Tag Wasser°C
0        Jan                          2                           -2                  65                    18               87          12        -
1        Feb                          3                           -2                  40                    15               84          21        -
2        Mär                          6                            0                  54                    13               81          34        -
```

Die Tabelle ist optisch nicht besonders ansprechend. Wir erreichen eine kompaktere Form, indem wir die Tabelle transponieren:
```python
df.set_index('Unnamed: 0',inplace=True) # Damit die Monatsnamen auch als Spaltenüberschriften erscheinen
print(df.transpose())
```
```
Unnamed: 0                  Jan Feb Mär Apr Mai Jun Jul Aug Sep Okt Nov Dez  Jahr
Temperatur °Cmax. Ømin. Ø     2   3   6  11  16  20  21  21  18  13   8   4   119
Temperatur °Cmax. Ømin. Ø.1  -2  -2   0   3   7  11  12  12  10   7   3   0    51
NiederschlagmmTage           65  40  54  52  57  69  79  69  66  67  86  74  1635
NiederschlagmmTage.1         18  15  13  14  12  14  15  16  15  17  18  18   185
relativeFeuchte              87  84  81  77  74  74  76  78  81  85  68  87    81
Sonneh/Tag                   12  21  34  55  74  76  71  71  49  33  17  11    44
Wasser°C                      -   -   -   -   -   -   -   -   -   -   -   -     -
```
**Datenstrukturen kategorisieren und faktorisieren**<br>

**Datentypen harmonisieren**<br>

**Datenstrukturen vergleichen**<br>

**Datenstrukturen entpivotieren**<br>

Eine nützliche Funktion für die Datenauswertung ist das Entpivotieren von Daten. Dabei werden die Spalten einer Tabelle zusammengefasst, heißt die Anzahl an Spalten sinkt, dafür vergrößert sich die Anzahl an Zeilen. Zum Entpivotieren benötigt man eine oder mehrere ID-Spalten und eine oder mehrere Werte-Spalten. Die Werte-Spalten werden in der entpivotierten Tabelle dann in einer Variable-Spalte und einer Werte-Spalte ausgegeben. Die Variable-Spalte entspricht dabei der ehemaligen Spaltenüberschrift und der Inhalt der Werte-Spalte dem zugehörigen Zellenwert. Wenn Spalten nicht in der ID -oder der Wertespalte aufgenommen werden, erscheinen sie nachher auch nicht in der entpivotierten Spalte.<br>
Fangen wir mit einem einfachen Beispiel an, indem wir unser eingangs angelegtes Dataframe entpivotieren:

```python
import pandas as pd 

dict={"A":pd.Series(["x","y","x","y"]),
    "B":pd.Series([1,2,1,2]),
    "C":pd.Series([3,4,5,6]),
    "D":pd.Series([10,20,30,40])}

df=pd.DataFrame(dict)

print (df.melt(id_vars=["A"],value_vars=["B","C","D"]))
```
```
    A variable  value
0   x        B      1
1   y        B      2
2   x        B      1
3   y        B      2
4   x        C      3
5   y        C      4
6   x        C      5
7   y        C      6
8   x        D     10
9   y        D     20
10  x        D     30
11  y        D     40
```
Wenn die ID-Spalte weggelassen wird, nimmt Python automatisch die Index-Spalte. Da die Spalte "A" nun weder als ID -noch als Wertespalte auftaucht, erscheint sie auch nicht mehr in der entpivotierten Tabelle.
```python
print (df.melt(value_vars=["B","C","D"]))
```
```
   variable  value
0         B      1
1         B      2
2         B      1
3         B      2
4         C      3
5         C      4
6         C      5
7         C      6
8         D     10
9         D     20
10        D     30
11        D     40
```

Es macht Sinn, die standardmäßige Beschriftung der Variablen -und Wertespalte andere Namen zu geben:
```python
print (df.melt(value_vars=["B","C","D"], var_name='Buchstabe', value_name='Werte'))
```
```
   Buchstabe  Werte
0          B      1
1          B      2
2          B      1
```
Abschließend noch ein praktischer Anwendungsfall für das Entpivotieren von Daten aus dem Bereich der Informationssicherheit. Beim [BSI-Grundschutz](https://www.bsi.bund.de/DE/Themen/ITGrundschutz/ITGrundschutzKompendium/itgrundschutzKompendium_node.html) werden für gängige Technologien relevante Gefährdungsszenarien in Kreuzreferenztabellen zusammengefasst, um einen Anhalt für weitergehende Risikoanalysen zu bekommen. Die Kreuzrefrenztabellen sind allerdings in ihrer Ursprungsform nur einer manuellen Auswertung zugänglich. Die Tabelle für den Baustein APP.1.1 (Office-Produkte) sieht im Original folgendermaßen aus:
```
            G 0.18 G 0.19 G 0.20 G 0.21 G 0.22 G 0.28 G 0.29 G 0.37 G 0.39 G 0.45 G 0.46
APP.1.1
APP.1.1.A01    NaN    NaN      X      X    NaN    NaN    NaN    NaN      X    NaN    NaN
APP.1.1.A02    NaN      X    NaN    NaN      X    NaN    NaN    NaN      X    NaN    NaN
APP.1.1.A03    NaN    NaN    NaN    NaN    NaN    NaN      X    NaN      X    NaN    NaN
APP.1.1.A04    NaN      X    NaN    NaN    NaN      X    NaN    NaN      X    NaN      X
APP.1.1.A05      X    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN
APP.1.1.A06      X    NaN      X      X    NaN      X    NaN    NaN    NaN      X      X
APP.1.1.A07      X    NaN      X    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN
APP.1.1.A08      X    NaN    NaN    NaN    NaN    NaN      X    NaN    NaN    NaN    NaN
APP.1.1.A09    NaN      X    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN    NaN
APP.1.1.A10      X    NaN    NaN    NaN    NaN      X    NaN    NaN    NaN    NaN    NaN
APP.1.1.A11    NaN    NaN      X      X    NaN      X      X    NaN      X      X    NaN
APP.1.1.A12      X      X    NaN    NaN    NaN    NaN      X    NaN    NaN    NaN      X
APP.1.1.A13    NaN    NaN      X    NaN    NaN      X    NaN    NaN      X    NaN    NaN
APP.1.1.A14    NaN      X    NaN    NaN      X    NaN    NaN      X    NaN    NaN      X
APP.1.1.A15    NaN      X      X    NaN      X    NaN    NaN      X    NaN    NaN      X
APP.1.1.A16    NaN    NaN      X    NaN      X    NaN    NaN    NaN      X    NaN      X
```
Dabei sind die Spaltenüberschriften die relevanten Gefährdungen für den Baustein und hinter dem Index verstecken sich die Anforderungen, mit denen der Gefährdung begegnet werden soll. Um leichter herauszufiltern, welche Gefährdung durch welche Anforderung behandelt wird, würden wir die Tabelle derart entpivotieren, dass die Anforderungen die ID-Spalte bleiben und alle weiteren Spalten werden als Werte-Spalten behandelt:
```python
file="App1.1.xlsx"
df=pd.read_excel(file)
df_melt=df.melt(id_vars=df.columns[0],value_vars=df.columns[1:],
        var_name='Gefährdung', value_name='Relevanz').set_index(df.columns[0])
print (df_melt.head(10))
```
Damit wird die Tabelle deutlich kompakter und lässt sich in Excel leichter nach der Relevanz filtern.
```
            Gefährdung Relevanz
APP.1.1
APP.1.1.A01     G 0.18      NaN
APP.1.1.A02     G 0.18      NaN
APP.1.1.A03     G 0.18      NaN
APP.1.1.A04     G 0.18      NaN
APP.1.1.A05     G 0.18        X
APP.1.1.A06     G 0.18        X
APP.1.1.A07     G 0.18        X
APP.1.1.A08     G 0.18        X
APP.1.1.A09     G 0.18      NaN
APP.1.1.A10     G 0.18        X
...
```

**Datenstrukturen pivotieren**<br>

Wie zu erwarten ist, handelt es sich beim pivotieren von Daten um das Gegenteil des entpivotierens.  Eine Spalte wird um 90 Grad geschwenkt und zu Spaltenüberschriften. Die Werte für die jeweiligen Spaltenüberschriften bilden die Werte-Spalten. Wir nehmen zum Anfang wieder unser eingangs erstelltes Dataframe und geben es zum besseren Verständnis noch einmal in Originalform aus:

``` python
print (df)
   A  B  C   D
0  x  1  3  10
1  y  2  4  20
2  x  1  5  30
3  y  2  6  40
```

Wir machen jetzt aus der Spalte "A" Spaltenüberschriften und nutzen die Spalte "B" für die Werte:

```python
print (df.pivot(columns="A",values=["B"]))
     B     
A    x    y
0  1.0  NaN
1  NaN  2.0
2  1.0  NaN
3  NaN  2.0
```

Aus der Spalte "A" ist nun die erste Zeile geworden, wobei python die doppelten Werte aggregiert hat. Die Werte aus der Spalte "B" hat python nun dem Index entsprechend auf die x -und y-Spalte aufgeteilt. Etwas unübersichtlich wird es allerdings, wenn wir die übrigen Spalten auch noch mit in die Pivot-Tabelle aufnehmen wollen.

```python
print (df.pivot(columns="A",values=["B","C","D"]))
     B         C          D      
A    x    y    x    y     x     y
0  1.0  NaN  3.0  NaN  10.0   NaN
1  NaN  2.0  NaN  4.0   NaN  20.0
2  1.0  NaN  5.0  NaN  30.0   NaN
3  NaN  2.0  NaN  6.0   NaN  40.0
```

Unsere Indexspalte "A" wird nun auf die drei Wertespalten aufgeteilt, was einer strukturierten Auswertung nicht unbedingt förderlich ist.

Die generelle pivot-Funktion tauscht Spalten und Zeilen einfach miteinander aus und kommt damit auch mit den Datentypen string oder object zurecht. Für die Verarbeitung von rein numerischen Informationen bietet sich die Funktion pivot_table an, die Daten ähnlich wie das von Excel bekannt ist, aggregiert. In der Standardeinstellung wird dafür der Mittelwert über numpy.mean verwendet. Über den Parameter aggfunc lassen sich aber auch weitere statistische Funktionen wie max, min, stdv oder sum verwenden.

```python
print(df.pivot_table(columns=["A"],values=["B","C","D"]))
A   x   y
B   1   2
C   4   5
D  20  30
```

**Daten zusammenführen**

Wer schon Erfahrungen hat mit SQL, der fühlt sich beim Verknüpfen von Tabellen zu Hause. Bei SQL join genannt, bei Excel Vlookup, heißt die Funktion bei python entweder join oder merge.<br>

Fangen wir mit einem praktischen Beispiel an. Wir reaktivieren noch einmal unser oben genutztes Dataframe mit Namen, Berufen und dem Alter.

```python
import pandas as pd
dict={"Name":pd.Series(["Hans","Karl","Petra","Julia"]),
    "Beruf":pd.Series(["Beamter","Kaufmann","Lehrerin","Musikerin"]),
    "Alter":pd.Series([35,42,50,61])}
df=pd.DataFrame(dict)
```

Zur Vereinfachung haben wir die oben genutzten Doppelnamen auf den ersten Namensbestandteil gekürzt. Wir haben jetzt die Aufgabe erhalten, eine Spalte mit dem Geschlecht hinzuzufügen. Das könnten wir bei den vier Zeilen noch manuell erledigen. Allerdings ist es nicht das Ziel bei der Datenanalyse, Daten manuell zu editieren. Zumal im Produktivbetrieb selten Daten mit nur vier Einträgen über unseren Tisch laufen. Wir brauchen also einen Mechanismus, der vom Namen automatisch auf das Geschlecht kommt.<br>

Wir nutzen dafür eine der vielen verfügbaren Namenslisten im Internet und verknüpfen die Namensspalten beider Tabellen miteinander. Bei einer Übereinstimmung wird das Geschlecht in einer zusätzlichen Spalte angezeigt.

```python
#Ich habe für dieses Beispiel die Daten von https://offenedaten-koeln.de/dataset/vornamen genutzt, die für deutsche Vornamen gute Ergebnisse liefern

df_names=pd.read_excel(r"Vornamen_Geschlecht.xlsx",header=0)
print (df.merge(df_names,how="left",on="Name"))

    Name      Beruf  Alter Geschlecht
0   Hans    Beamter     35          m
1   Karl   Kaufmann     42          m
2  Petra   Lehrerin     50          w
3  Julia  Musikerin     61          w
```

Wir lesen zuerst die Exceltabelle ein, die aus den beiden Spalten "Name" und "Geschlecht" besteht. Die Spalte "Name" kommt in beiden Datensätzen vor und kann für die Verknüpfung genutzt werden. Der Parameter how="left" besagt, dass die Einträge im Quelldatensatz (hier das Dataframe df) führend sind. Diese werden versucht, mit den Daten des Zieldatensatzes zu verknüpfen. Das Verhalten wird deutlich, wenn wir dem Quelldatensatz einen Eintrag hinzufügen, der sich bestimmt nicht in dem Datensatz df_names findet.

```python
df=df.append({"Name":"R2D2","Beruf":"Roboter","Alter":0},ignore_index=True)

    Name      Beruf  Alter Geschlecht
0   Hans    Beamter     35          m
1   Karl   Kaufmann     42          m
2  Petra   Lehrerin     50          w
3  Julia  Musikerin     61          w
4   R2D2    Roboter      0        NaN
```

R2D2 wird zwar in der Ausgabe angezeigt, allerdings ohne Geschlecht, da sich der Name nicht in der Zieltabelle findet. Mit dem Parameter how="right" können wir die Liste der Vornamen mit Geschlecht zur Quelltabelle machen, das Verhalten wird also umgedreht

```python
print (df.merge(df_names,how="right",on="Name").head(10))

     Name      Beruf  Alter Geschlecht
0    Hans    Beamter   35.0          m
1    Karl   Kaufmann   42.0          m
2   Petra   Lehrerin   50.0          w
3   Julia  Musikerin   61.0          w
4   Marie        NaN    NaN          w
5  Sophie        NaN    NaN          w
6   Maria        NaN    NaN          w
7   Maria        NaN    NaN          m
8    Noah        NaN    NaN          m
9  Emilia        NaN    NaN          w
```

R2D2 ist nun wieder verschwunden, da er nicht in der "neuen" Quelltabelle (df_names) auftaucht. Beruf und Alter tauchen jetzt auch nur noch in den Einträgen auf, die sich in unserer "alten" Quelltabelle (df) befinden, da es in df_names keine derartigen Informationen gibt. Mit dem Parameter .head(10) werden nur die ersten 10 Einträge angezeigt. Die neue Quelltabelle df_names würde ansonsten über einige Seiten weiterlaufen.

Probieren wir noch die weiteren Verküpfungsoptionen durch:

```python
# Mit inner werden nur die Daten angezeigt, die in beiden Datensätzen vorkommen
print (df.merge(df_names,how="inner",on="Name"))

    Name      Beruf  Alter Geschlecht
0   Hans    Beamter     35          m
1   Karl   Kaufmann     42          m
2  Petra   Lehrerin     50          w
3  Julia  Musikerin     61          w

#Mit outer werden alle Daten angezeigt und nur zugeordnet, wenn möglich
print (df.merge(df_names,how="outer",on="Name").head(10))

     Name      Beruf  Alter Geschlecht
0    Hans    Beamter   35.0          m
1    Karl   Kaufmann   42.0          m
2   Petra   Lehrerin   50.0          w
3   Julia  Musikerin   61.0          w
4    R2D2    Roboter    0.0        NaN
5   Marie        NaN    NaN          w
6  Sophie        NaN    NaN          w
7   Maria        NaN    NaN          w
8   Maria        NaN    NaN          m
9    Noah        NaN    NaN          m
```

Wer aus der SQL-Welt kommt, kennt eher den join-Befehl. Wo liegt also der Unterschied zwischen join und merge bei python? Beim merge lässt sich das Quell-Dataframe nur mit einem Ziel-Dataframe - oder Series verknüpfen. Dafür ist man aber bei der Auswahl der zu verknüpfenden Spalten flexibler.  Beim join lassen sich mehrere Ziel-Dataframes mit dem Quell-Dataframe verknüpfen. Allerdings ist man bei den Ziel-Dataframes immer auf den Index angewiesen.

Schauen wir uns das bei einem einfachen Beispiel an, bei dem am Schluss dasselbe Ergebnis herauskommen soll, wie im vorangegangenen Abschnitt. Zuerst bauen wir uns drei Dataframes auf, die nur in der Spalte "Name" übereinstimmen.

```python
import pandas as pd 

dict1={"Name":pd.Series(["Hans","Karl","Petra","Julia"]),"Beruf":pd.Series(["Beamter","Kaufmann","Lehrerin","Musikerin"])}
dict2={"Name":pd.Series(["Hans","Karl","Petra","Julia"]),"Alter":pd.Series([35,42,50,61])}
dict3={"Name":pd.Series(["Hans","Karl","Petra","Julia"]),"Geschlecht":pd.Series(["m","m","w","w"])}

df1=pd.DataFrame(dict1)
df2=pd.DataFrame(dict2)
df3=pd.DataFrame(dict3)
```

Im Ergebnis soll in der Ausgabe pro Zeile der Name, der Beruf, Alter und Geschlecht ausgegeben werden. Tasten wir uns langsam an dieses Ziel heran.

```python
print (df1.join([df2,df3]))

ValueError: Indexes have overlapping values: Index(['Name'], dtype='object')
```

Ok, python scheint ein Problem damit zu haben, dass in der Spalte "Name" überlappende Werte zu finden sind. Versuchen wir also, die Spalte "Name" als Schlüsselspalte zu verwenden.

```python
print (df1.join([df2,df3],on="Name"))

ValueError: Joining multiple DataFrames only supported for joining on index
```

Zum Glück sind die Fehlermeldungen aussagekräftiger, als bei manch einem Betriebssystem. Setzen wir also den Index auf die Spalte "Name" und starten noch einen Versuch.

```python
df1=pd.DataFrame(dict1).set_index("Name")
df2=pd.DataFrame(dict2).set_index("Name")
df3=pd.DataFrame(dict3).set_index("Name")

print (df1.join([df2,df3]))

           Beruf  Alter Geschlecht
Name
Hans     Beamter     35          m
Karl    Kaufmann     42          m
Petra   Lehrerin     50          w
Julia  Musikerin     61          w
```

Das war das Ergebnis, dass wir erwartet hatten. <br>

Es bleibt also festzuhalten, dass die merge-Funktion flexibler ist, wenn nur zwei Datensätzen im Spiel sind. Die join-Funktion kann auch mit mehreren Datensätzen umgehen, dafür allerdings nur indexbasiert.
