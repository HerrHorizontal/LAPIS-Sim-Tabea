In diesem Repo sind meine Skripte zum submittieren der verschiedenen Simulationen sowie deren Auswertung. 

In beiden Fällen die die Skripte hier eher zur Inspiration gedacht und sie werden wahrscheinlich auch nicht direkt funktionieren. 
Für die Auswertung solltest du dir einen besseren workflow ausdenken, was du hier findest ist nur ein verzweifelter kürzester Weg, aber eigentlich kann man nicht effizient damit arbeiten. 

Zum Submittieren: 

Das Beispielskript zum submittieren ist simulation_full.py, simulate_equalized_jobs.py ist im Prinzip sehr ähnlich, in jedem wird über die zwischen verschiedenen Simulationsszenarien variierten Größen (cache hitrates, Konfigurationen der job und machine ClassAds, job inputs) iteriert und dabei eine feste Namenskonvention für die job inputs ausgenutzt.
Jedes Szenario entspricht einem job. Die Funktionalitäten um diesen Job tatsächlich zu submittieren liegen im module tools. 
Zentral ist dabei der SimulationSubmitterCustom (submit_simulation.py), hier werden die python bindings von HTCondor zum submittieren der jobs benutzt. Die dem Job übergebene ClassAd wird zuvor als dict (job_description) erstellt. 
Die Executable des jobs ist tools/run_lapis_custom_simulation.sh,

Zum Analysieren: 

Um zu beobachten was während der Simulation passiert ist, werde die Einträge aus dem Monitoring von lapis (log file) verwendet. Da diese von den worker nodes aus nicht direkt an die influxDB gesendet werden konnten, habe ich einen Umweg über XML files genommen.
Für das Einlesen der log files gibt es die bash scripts digest_logfile.sh bzw digest_directory.sh (liest alle files im directory und subdirectories ein). 
In der Datenbank werden die einzelnen simulations run durch den lapis tag (z.B. lapis-1581792738.8862092) identifiziert. Damit klar ist welcher tag welchem Simulationsszenario (job file, hitrates, coordination config, cluster size) entspricht erstellt extract_tag_mapping.py ein mapping zwischen dem Simulationsszenario und den tags. Das ist einfach möglich, da sowohl tag als auch die anderen Angabne  im out files des HTConodor Jobs vorkommen. 
Dieses Vorgehen ist ein bisschen umständlich. Es ist aber wahrscheinlich fail safer als den jobs die Bezeichnung für ihr Simulationsszenario zusätzlich zu übergeben. Besser wäre es, wenn der Simulator aufgrund seiner configuration alle relevanten Parameter kennen und das Szenario selbst ins eigene Monitoring loggen könnte. 

Für die Analyse/das Erstellen von Ergebnisplots werden unter Verwendung dieses Mappings Monitoringdaten von den zu untersuchenden Szenarien aus der influxDB in panda data frames exportiert. 
Die Datenbank liegt auf der portal1, falls es sie noch gibt. Auch wenn man nicht alle Metriken (job event, pipe event etc) importiert ist das ein sehr zeitaufwendiger Prozess. Vielleicht gibt es dafür bessere Lösungen. der influxDB geladen. 
Die dataframes können dann weiter verwendet, analysiert und geplottet werden. 
Beispielhaft sieht man das in make_plots_jobs_on_caching_cluster() (analysis/cross_check_plots.py). 
