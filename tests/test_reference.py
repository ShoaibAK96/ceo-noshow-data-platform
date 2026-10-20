import importlib.util,json,sqlite3,tempfile,unittest
from pathlib import Path
from src.reference_demo import parse,run
ROOT=Path(__file__).resolve().parents[1]
SAMPLE=ROOT/'sample_data/DEMO-NOSHOWDATA-20260115100000.txt'
spec=importlib.util.spec_from_file_location('transfer',ROOT/'src/transfer.py');transfer=importlib.util.module_from_spec(spec);spec.loader.exec_module(transfer)
class ReferenceTests(unittest.TestCase):
    def test_duplicate_rows_dates_identifiers(self):
        rows=parse(SAMPLE.read_text(),SAMPLE.name)
        self.assertEqual(len(rows),1);self.assertEqual(rows[0]['report_date'],'2026-01-15')
        self.assertEqual(rows[0]['CLINICID'],'SYN-C01');self.assertIsNone(rows[0]['ADVANCEDT'])
    def test_malformed_filename_calendar_and_required_column(self):
        for name,body in [('bad.txt',SAMPLE.read_text()),('DEMO-NOSHOWDATA-20260230100000.txt',SAMPLE.read_text()),(SAMPLE.name,SAMPLE.read_text().replace('CLINICID','UNKNOWN'))]:
            with self.subTest(name=name),self.assertRaises(ValueError):parse(body,name)
    def test_strict_integer_and_datetime(self):
        for old,new in [('|0|N|','|wrong|N|'),('15/01/2026 09:00:00','bad-date')]:
            with self.assertRaises(ValueError):parse(SAMPLE.read_text().replace(old,new),SAMPLE.name)
    def test_replay_and_same_day_duplicate(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(run(SAMPLE,d),{'status':'SUCCESS','rows':1})
            self.assertEqual(run(SAMPLE,d)['status'],'DUPLICATE')
            other=Path(d)/'DEMO-NOSHOWDATA-20260115110000.txt';other.write_text(SAMPLE.read_text())
            self.assertEqual(run(other,d)['status'],'DUPLICATE')
    def test_injected_failure_does_not_commit_then_retry(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(RuntimeError):run(SAMPLE,d,True)
            with sqlite3.connect(Path(d)/'reference.sqlite') as db:self.assertEqual(db.execute('select count(*) from business').fetchone()[0],0)
            self.assertEqual(run(SAMPLE,d)['rows'],1)
    def test_ambiguous_existing_data_stops(self):
        with tempfile.TemporaryDirectory() as d:
            run(SAMPLE,d)
            with sqlite3.connect(Path(d)/'reference.sqlite') as db:db.execute('delete from history')
            with self.assertRaises(RuntimeError):run(SAMPLE,d)
    def test_transfer_copy_state_and_collision(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);src=p/'source.txt';src.write_text('synthetic');dest=p/'incoming';dest.mkdir();state=p/'state.sqlite'
            transfer.initialize_database(state);copied=transfer.copy_file(src,dest,1,False)
            self.assertEqual(copied.read_bytes(),src.read_bytes());self.assertTrue(src.exists())
            transfer.record_state(state,src,copied,'COPIED','synthetic')
            self.assertTrue(transfer.already_copied(state,src))
            with self.assertRaises(FileExistsError):transfer.copy_file(src,dest,1,False)
            self.assertFalse((dest/'source.txt.part').exists())
if __name__=='__main__':unittest.main()
