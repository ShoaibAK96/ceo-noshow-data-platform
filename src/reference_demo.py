"""Reconstructed synthetic demo. CSV/SQLite stand in for service boundaries, not Delta."""
import argparse,csv,json,re,sqlite3,uuid
from datetime import datetime,timezone
from pathlib import Path
COLUMNS=['CLINICID','DOCTORID','DOCTORNAME','APPTDTTM','APPTID','APPTNATUREID','VISITURN','BOOKINGDT','SPECIALTYID','SCHLDDT','ADMINSVCID','ADVANCE','ADVADJUST','ADVANCEDT','ADVADJUSTPYBDT']
def parse(text,name):
    match=re.fullmatch(r'DEMO-NOSHOWDATA-(\d{14})\.txt',name)
    if not match:raise ValueError('Invalid synthetic filename')
    date=datetime.strptime(match[1],'%Y%m%d%H%M%S').date().isoformat()
    lines=[line.strip() for line in text.lstrip('\ufeff').splitlines() if line.strip()]
    if len(lines)<4:raise ValueError('Source requires envelope, columns and data')
    header=lines[1].split('|')
    if header[0]!='C':raise ValueError('Missing C header')
    columns=[s.strip().upper() for s in header[1:]]
    if len(columns)!=len(set(columns)):raise ValueError('Duplicate header')
    if set(COLUMNS)-set(columns):raise ValueError('Missing columns')
    rows=[]
    for line in lines[2:-1]:
        parts=[s.strip() for s in line.split('|')]
        if parts[0]!='D':continue
        values=parts[1:]+['']*max(0,len(columns)-len(parts)+1)
        record=dict(zip(columns,values));out={'report_date':date}
        for col in COLUMNS:
            value=record[col]
            if col=='ADVANCE':value=int(value) if value else None
            elif col in ['APPTDTTM','BOOKINGDT','SCHLDDT','ADVANCEDT','ADVADJUSTPYBDT']:
                value=datetime.strptime(value,'%d/%m/%Y %H:%M:%S').isoformat() if value else None
            out[col]=value
        rows.append(out)
    if not rows:raise ValueError('No D records')
    return list({json.dumps(r,sort_keys=True):r for r in rows}.values())

def run(source,output,fail_before_commit=False):
    source=Path(source);output=Path(output);output.mkdir(parents=True,exist_ok=True)
    rows=parse(source.read_text(),source.name);day=rows[0]['report_date'];run_id=str(uuid.uuid4())
    with sqlite3.connect(output/'reference.sqlite') as db:
        db.execute('CREATE TABLE IF NOT EXISTS business(source_file TEXT, run_id TEXT, payload TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS history(source_file TEXT, report_date TEXT, status TEXT)')
        if db.execute("SELECT 1 FROM history WHERE status='SUCCESS' AND (source_file=? OR report_date=?)",(source.name,day)).fetchone():
            return {'status':'DUPLICATE','rows':0}
        if db.execute('SELECT 1 FROM business WHERE source_file=?',(source.name,)).fetchone():
            raise RuntimeError('Existing rows require reconciliation')
        for row in rows:
            row.update(source_file=source.name,run_id=run_id,load_timestamp=datetime.now(timezone.utc).isoformat())
            db.execute('INSERT INTO business VALUES(?,?,?)',(source.name,run_id,json.dumps(row)))
        if fail_before_commit:raise RuntimeError('Injected failure before local transaction commit')
        db.execute('INSERT INTO history VALUES(?,?,?)',(source.name,day,'SUCCESS'))
    with (output/'synthetic_output.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=rows[0].keys());writer.writeheader();writer.writerows(rows)
    return {'status':'SUCCESS','rows':len(rows)}
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args();print(json.dumps(run(args.source,args.output)))
