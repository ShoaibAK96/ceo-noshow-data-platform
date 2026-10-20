from PIL import Image,ImageDraw,ImageFont
from pathlib import Path
R=Path(__file__).resolve().parent
OUT=R/'docs/images' if (R/'README.md').exists() else R/'noshow_showcase/public/docs/images'
OUT.mkdir(parents=True,exist_ok=True)
FONT='/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf'; BOLD=FONT.replace('.ttf','-Bold.ttf')
NAVY='#112D3B'; TEAL='#007E80'; GOLD='#A85E10'; INK='#263F4D'; BG='#F2F6F6'; WHITE='#FFFFFF'; RED='#A33A49'
def font(n,b=False):return ImageFont.truetype(BOLD if b else FONT,n)
def line(d,xy,fill=TEAL):
 d.line(xy,fill=fill,width=6);x,y=xy[-1];d.polygon([(x,y),(x-11,y-17),(x+11,y-17)],fill=fill)
def text(d,xy,t,n=28,color=INK,b=False):d.text(xy,t,font=font(n,b),fill=color)
def card(d,box,num,title,lines,color=TEAL):
 x,y,x2,y2=box;d.rounded_rectangle(box,22,fill=WHITE,outline='#CBDADD',width=2);d.rounded_rectangle((x,y,x+12,y2),6,fill=color)
 text(d,(x+32,y+25),num,24,color,True);text(d,(x+96,y+22),title,33,NAVY,True)
 for i,l in enumerate(lines):text(d,(x+36,y+86+i*42),l,27)
def header(d,w,title,sub):
 d.rectangle((0,0,w,220),fill=NAVY);text(d,(85,45),'CEO DASHBOARD  /  NOSHOW',28,'#75D4C8',True);text(d,(85,100),title,49,WHITE,True);text(d,(85,170),sub,25,'#D5E3E7')
im=Image.new('RGB',(2200,1250),BG);d=ImageDraw.Draw(im)
header(d,2200,'From operational files to reporting inputs','Python  •  Power Automate  •  Office Scripts  •  Microsoft Fabric  •  Delta Lake')
card(d,(85,270,710,535),'01','Collect TXT files',['Preserve upstream source','SQLite transfer history','Partial copy + size checks'])
card(d,(785,270,1410,535),'02','Prepare Excel',['Validate + detect duplicates','SharePoint + Office Scripts','15 fields + reporting date'])
card(d,(1485,270,2110,535),'03','Load in Fabric',['Metadata-driven PySpark','Typed Delta + audit lineage','Logs, archive + scoped rollback'])
for x in [710,1410]:
 d.line((x+8,400,x+62,400),fill=TEAL,width=6);d.polygon([(x+64,400),(x+47,389),(x+47,411)],fill=TEAL)
d.rounded_rectangle((85,585,2110,765),22,fill='#FFF2DD',outline='#E7CEAA',width=2)
text(d,(120,610),'INTEGRATION HANDOFF  /  deployment evidence still required',31,GOLD,True)
text(d,(120,663),'Completed workbook → recommended ready queue → Copy activity → notebook',32)
text(d,(120,713),'Queue deployment and exact pipeline export are unverified; arrows describe the intended chain.',26)
d.rounded_rectangle((85,810,1070,1080),22,fill=WHITE)
text(d,(120,838),'Reliability at each boundary',33,NAVY,True)
for i,t in enumerate(['Transfer metadata checks ≠ content hashes','Daily output duplicates → exceptions','Fabric replay guards + row deduplication','Log failure after archive → manual reconciliation']):text(d,(120,900+i*40),t,26)
d.rounded_rectangle((1120,810,2110,1080),22,fill=WHITE)
text(d,(1155,838),'What the evidence establishes',33,NAVY,True)
for i,t in enumerate(['Original code + metadata are available','Three stages confirmed complete by the user','No measured savings, volume or uptime claimed','Public examples contain synthetic records only']):text(d,(1155,900+i*40),t,26)
text(d,(85,1140),'Solid: implemented components   |   Amber: unverified integration   |   Exceptions: quarantine / recovery',26)
text(d,(85,1190),'Architecture overview • Component completion does not certify a live end-to-end run',25,TEAL,True)
im.save(OUT/'NoShow_Summary.png')
im=Image.new('RGB',(2400,3060),BG);d=ImageDraw.Draw(im)
header(d,2400,'The complete engineering journey','Execution order • Evidence-backed implementation • Explicit integration and production boundaries')
stages=[
('01','Source collection | Python on Windows',['Upstream application deposits pipe-delimited TXT files; source remains untouched.','Filter eligible names/extensions; wait for minimum age; consult SQLite metadata state.','Copy to .part, flush, verify size/mtime, rename, then record transfer outcome.'],TEAL),
('02','Arrival and validation | Power Automate',['Gateway + File System connector monitor ingestion folder; read content using path.','Preserve raw source in SharePoint; check successful source and daily-output history.','Invalid names or duplicate deliveries follow exception and logging branches.'],TEAL),
('03','Workbook preparation | Office Scripts',['Copy blank workbook; run script against the created workbook identifier.','Parse C/D records; retain 15 fields, pad trailing omissions and add report_date.','Original script derives the date from filename; Fabric accepts the supplied workbook date.'],TEAL),
('04','Delivery and source lifecycle',['Copy completed workbook back to server; avoid reading a workbook still being written.','Archive ingestion copies before cleanup; preserve upstream application originals.','Documented Excel log outcomes: SUCCESS, FAILED and DUPLICATE.'],TEAL),
('05','Server-to-Lakehouse handoff | integration boundary',['Intended chain: completed workbook → Copy activity → Lakehouse Incoming → notebook.','Ready/sent queues and per-file acknowledgement were recommended hardening.','Pipeline export, live queue deployment and end-to-end run evidence are unavailable.'],GOLD),
('06','Validation and transformation | Fabric V3',['Validate configured sheet, required columns, normalized headers and strict types.','Preserve identifiers as strings; parse operational timestamps with day-first rules.','Remove exact business-row duplicates before adding file/run/time audit fields.'],TEAL),
('07','Delta load, replay guards and archive',['SUCCESS history prevents replay; existing data without success history blocks blind append.','Append typed business rows; maintain separate file and error log tables.','Load/archive failure: compensating rollback scoped to source_file + run_id.'],TEAL),
('08','Exception handling and operator recovery',['Quarantine invalid/duplicate intake; investigate same-name changed file collisions.','If final SUCCESS logging fails, data stays loaded and workbook stays archived.','Reconcile business rows, archive and logs before replay; do not reset state blindly.'],RED),
('09','Evidence boundary and future acceptance',['User confirms three completed workstreams; original sources verify implemented logic.','No quantitative production impact or dashboard publication is established here.','Still needed: deployment exports, actual schedule, run evidence and recovery acceptance.'],GOLD)]
y=265
for i,(num,title,lines,color) in enumerate(stages):
 card(d,(85,y,2315,y+255),num,title,lines,color)
 if i<8:line(d,[(1200,y+260),(1200,y+289)],color)
 y+=295
text(d,(85,2940),'Legend: teal = implemented / documented   •   amber = unverified / future   •   red = exception / recovery',27)
text(d,(85,2990),'No historical-load or locked branch is evidenced. This is an execution journey, not a development-date timeline.',25)
im.save(OUT/'NoShow_Detailed.png')
print(OUT)
