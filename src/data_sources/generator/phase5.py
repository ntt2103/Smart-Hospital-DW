"""Deterministic 180-day hospital workload; CSV chunks and a disk-backed FK index.

Volume is derived from encounters/day, never padded to a requested total row count.
Run: python -m data_sources.generator.phase5 --output artifacts/generated/phase5
"""
import argparse
import bisect
import csv
import hashlib
import io
import json
import math
import sqlite3
import shutil
import time
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

from data_sources.generator.historical import CORE_TABLES, LOAD_ORDER, VN, iso, moment


@dataclass(frozen=True)
class Config:
    encounters_per_day: int = 6000
    horizon_days: int = 180
    as_of: str = '2026-11-02T00:00:00+07:00'
    seed: int = 20261102
    inpatient_fraction: float = 1 / 26
    patients_per_encounter: float = 200000 / 1040000
    appointments_per_encounter: float = 700000 / 1040000
    labs_per_encounter: float = 2.8
    mean_los_days: float = 4
    target_bed_utilization: float = .75
    doctors: int = 600
    lab_test_types: int = 20
    diagnoses: int = 100

    def counts(self):
        n = self.encounters_per_day * self.horizon_days
        ni = round(n * self.inpatient_fraction)
        return {'patients': max(1, round(n*self.patients_per_encounter)),
                'appointments': round(n*self.appointments_per_encounter),
                'encounters': n, 'outpatient_encounters': n-ni, 'inpatient_encounters': ni,
                'beds': max(64, math.ceil(ni*self.mean_los_days/self.horizon_days/self.target_bed_utilization)),
                'lab_orders': round(n*self.labs_per_encounter)}

    def validate(self):
        if self.encounters_per_day < 1 or self.horizon_days < 2 or not 0 <= self.inpatient_fraction < .5:
            raise ValueError('Positive daily volume, horizon >=2 and inpatient fraction <.5 required')
        if moment(self.as_of).utcoffset() is None:
            raise ValueError('as_of requires timezone')
        if not 0 < self.target_bed_utilization < 1 or self.mean_los_days <= 0:
            raise ValueError('Invalid bed capacity policy')
        if self.doctors < 20 or self.lab_test_types < 1 or self.diagnoses < 4:
            raise ValueError('Catalogs too small')
        if self.counts()['appointments']*.78 > self.counts()['outpatient_encounters']:
            raise ValueError('Too many attended appointments for OP capacity')


class Workload:
    """Only catalogs and bed calendars live in memory; encounter contexts live on disk."""
    weights = (3, 3, 2, 2, 1, 1, 1, 1, 1, 1)

    def __init__(self, config, index_path):
        config.validate()
        self.config, self.counts = config, config.counts()
        self.end = moment(config.as_of).astimezone(VN)
        self.start = self.end-timedelta(days=config.horizon_days)
        self.db = sqlite3.connect(index_path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA cache_size=-8192')
        self.db.execute('DROP TABLE IF EXISTS contexts')
        self.db.execute('DROP TABLE IF EXISTS attended')
        self.db.execute('DROP TABLE IF EXISTS assignments')
        self.db.execute('DROP TABLE IF EXISTS requests')
        self.db.execute('CREATE TABLE attended(id INTEGER PRIMARY KEY,patient INTEGER,dept INTEGER,doctor INTEGER,scheduled TEXT)')
        self.db.execute('CREATE TABLE contexts(id INTEGER PRIMARY KEY,patient INTEGER,dept INTEGER,doctor INTEGER,started TEXT,finished TEXT,kind TEXT)')
        self.db.execute('CREATE INDEX context_start_idx ON contexts(started)')
        self.db.execute('CREATE TABLE assignments(id INTEGER PRIMARY KEY,bed INTEGER,encounter INTEGER,started TEXT,finished TEXT)')
        self.db.execute('CREATE TABLE requests(id INTEGER PRIMARY KEY,proposed TEXT,parent INTEGER,patient INTEGER,dept INTEGER)')
        self.day_weights = []
        acc = 0
        for day in range(config.horizon_days):
            dt = self.start+timedelta(days=day)
            acc += (1.1, 1.1, 1.1, 1.1, 1.05, .8, .75)[dt.weekday()] * (1+.1*math.sin(day/13))
            self.day_weights.append(acc)
        self.doctor_depts = {i: self.department(i) for i in range(1, config.doctors+1)}
        self.dept_doctors = {d: [i for i, dept in self.doctor_depts.items() if dept == d] for d in range(1, 11)}
        self.bed_depts = {i: self.department(i) for i in range(1, self.counts['beds']+1)}
        self.maintenance_beds=set(sorted(self.bed_depts,key=lambda bed:self.number(bed,83))[:self.counts['beds']//8])
        self.maintenance_start = self.start+timedelta(days=config.horizon_days//2)
        self.maintenance_end = self.maintenance_start+timedelta(days=min(7, max(1, config.horizon_days//10)))

    def number(self, key, salt=0):
        x = (int(key)+self.config.seed*0x9e3779b1+salt*0x85ebca6b) & ((1 << 64)-1)
        x = ((x ^ (x >> 30))*0xbf58476d1ce4e5b9) & ((1 << 64)-1)
        x = ((x ^ (x >> 27))*0x94d049bb133111eb) & ((1 << 64)-1)
        return x ^ (x >> 31)

    def department(self, i):
        # Cycling weighted mapping also guarantees catalog coverage of every department.
        k = (i-1) % sum(self.weights)
        for d, weight in enumerate(self.weights, 1):
            if k < weight:
                return d
            k -= weight

    def doctor(self, dept, i):
        pool = self.dept_doctors[dept]
        return pool[self.number(i, 7) % len(pool)]

    def slot(self, i, total, salt=0):
        fraction = (i-.5)/max(total, 1)
        day = bisect.bisect_left(self.day_weights, fraction*self.day_weights[-1])
        hours = (8, 8, 8, 9, 9, 9, 10, 10, 13, 15)
        return (self.start+timedelta(days=min(day,self.config.horizon_days-1))).replace(
            hour=hours[self.number(i,salt) % len(hours)], minute=self.number(i,salt+1) % 60,
            second=0, microsecond=0)

    def row(self, created=None, updated=None, **values):
        created = created or self.start-timedelta(days=365*91)
        values.update(created_at=iso(created), updated_at=iso(updated or created))
        return values

    def target(self, i):
        return (30,45,60,90,120,180,240,360)[(i-1) % 8]

    def iter_table(self, table):
        c, n = self.config, self.counts
        if table == 'departments':
            for i in range(1,11):
                yield self.row(department_id=i,department_code=f'DEP-{i:02}',department_name=f'Synthetic Department {i:02}',is_active=True)
        elif table == 'patients':
            for i in range(1,n['patients']+1):
                yield self.row(patient_id=i,patient_code=f'SYN-{i:08}',date_of_birth=str((self.start-timedelta(days=1+self.number(i) % (90*365))).date()),gender=('FEMALE','MALE','OTHER','UNKNOWN')[self.number(i,1)%4],is_active=True)
        elif table == 'doctors':
            for i in range(1,c.doctors+1):
                yield self.row(doctor_id=i,doctor_code=f'DOC-{i:04}',department_id=self.doctor_depts[i],is_active=True)
        elif table == 'beds':
            for i in range(1,n['beds']+1):
                yield self.row(bed_id=i,bed_code=f'BED-{i:05}',department_id=self.bed_depts[i],bed_type='ICU' if i%12 == 0 else 'REGULAR',commissioned_at=iso(self.start))
        elif table == 'bed_status_history':
            key = 0
            for bed in range(1,n['beds']+1):
                spans = [(self.start,None,'ACTIVE')]
                if bed in self.maintenance_beds:
                    spans = [(self.start,self.maintenance_start,'ACTIVE'),(self.maintenance_start,self.maintenance_end,'MAINTENANCE'),(self.maintenance_end,None,'ACTIVE')]
                for left,right,status in spans:
                    key += 1
                    yield self.row(created=self.start,updated=right or left,bed_status_history_id=key,bed_id=bed,status=status,effective_from=iso(left),effective_to=iso(right) if right else None,reason='Synthetic maintenance schedule')
        elif table == 'lab_test_types':
            for i in range(1,c.lab_test_types+1):
                yield self.row(lab_test_type_id=i,test_code=f'TEST-{i:03}',test_name=f'Synthetic Test {i:03}',default_tat_minutes=self.target(i),is_active=True)
        elif table == 'diagnoses':
            for i in range(1,c.diagnoses+1):
                yield self.row(diagnosis_id=i,diagnosis_code=f'SYN-DX-{i:03}',diagnosis_name=f'Synthetic diagnosis {i:03}',is_active=True)
        elif table == 'appointments':
            na = n['appointments']
            attended = sum(1 for i in range(1,na+1) if (i-1)%100<76 or (i-1)%100==94)
            ordinal = 0
            for i in range(1,na+1):
                r = (i-1)%100
                status = 'ATTENDED' if r<76 or r==94 else 'NO_SHOW' if r<86 else 'CANCELLED' if r<93 else 'RESCHEDULED' if r==93 and i<na else 'SCHEDULED'
                predecessor = i-1 if r==94 else None
                patient = 1+self.number(i-1 if predecessor else i,2)%n['patients']
                dept = self.department(self.number(i,3))
                doctor = self.doctor(dept,i)
                if status=='ATTENDED':
                    ordinal += 1
                    scheduled = self.slot(ordinal,attended,11)
                    self.db.execute('INSERT INTO attended VALUES(?,?,?,?,?)',(i,patient,dept,doctor,iso(scheduled)))
                elif status=='RESCHEDULED':
                    scheduled = self.slot(ordinal+1,attended,11)-timedelta(hours=2)
                elif status=='SCHEDULED':
                    scheduled = self.end-timedelta(hours=2,minutes=i%60)
                else:
                    scheduled = self.slot(i,na,13)
                    if status=='NO_SHOW':
                        scheduled = min(scheduled,self.end-timedelta(hours=25))
                changed = scheduled+timedelta(hours=24) if status=='NO_SHOW' else scheduled
                if status=='SCHEDULED': changed = scheduled-timedelta(days=1)
                yield self.row(created=scheduled-timedelta(days=1),updated=changed,appointment_id=i,patient_id=patient,department_id=dept,doctor_id=doctor,scheduled_at=iso(scheduled),status=status,status_changed_at=iso(changed),rescheduled_from_appointment_id=predecessor)
            self.db.commit()
        elif table == 'encounters':
            attended = self.db.execute('SELECT * FROM attended ORDER BY id')
            for i in range(1,n['outpatient_encounters']+1):
                appt = next(attended,None)
                if appt:
                    aid,patient,dept,doctor,checked = appt
                    checked = moment(checked)
                else:
                    aid = None
                    patient = 1+self.number(i,2)%n['patients']
                    dept = self.department(self.number(i,3))
                    doctor = self.doctor(dept,i)
                    checked = self.slot(i-n['appointments']*.77,n['outpatient_encounters']-n['appointments']*.77,19)
                began = checked+timedelta(minutes=5+self.number(i,4)%91)
                completed = began+timedelta(minutes=8+self.number(i,5)%23)
                self.db.execute('INSERT INTO contexts VALUES(?,?,?,?,?,?,?)',(i,patient,dept,doctor,iso(began),iso(completed),'OUTPATIENT'))
                yield self.row(created=checked,updated=completed,encounter_id=i,patient_id=patient,department_id=dept,doctor_id=doctor,appointment_id=aid,parent_encounter_id=None,encounter_type='OUTPATIENT',status='COMPLETED',status_changed_at=iso(completed),checked_in_at=iso(checked),consultation_started_at=iso(began),consultation_completed_at=iso(completed),admitted_at=None,discharged_at=None)
            self.db.commit()
            yield from self.inpatients()
        elif table == 'bed_assignments':
            for key,bed,encounter,left,right in self.db.execute('SELECT * FROM assignments ORDER BY id'):
                yield self.row(created=moment(left),updated=moment(right or left),bed_assignment_id=key,bed_id=bed,encounter_id=encounter,assigned_at=left,released_at=right)
        elif table == 'lab_orders':
            for i in range(1,n['lab_orders']+1):
                eid = 1+self.number(i,31)%n['encounters']
                _,_,dept,_,started,_,_ = self.db.execute('SELECT * FROM contexts WHERE id=?',(eid,)).fetchone()
                ordered = moment(started)+timedelta(minutes=2)
                test = 1+self.number(i,32)%c.lab_test_types
                target = self.target(test)
                collected,processing = ordered+timedelta(minutes=3),ordered+timedelta(minutes=5)
                result = ordered+timedelta(minutes=6+self.number(i,33)%max(1,round(target*1.4)))
                result = max(result,processing)
                state = 'CANCELLED' if i%20==0 else 'ORDERED' if i%101==0 else 'PROCESSING' if i%97==0 else 'COMPLETED'
                if state=='COMPLETED' and result>=self.end:
                    state='PROCESSING' if processing<self.end else 'ORDERED'
                collected = collected if state in ('PROCESSING','COMPLETED') else None
                processing = processing if state in ('PROCESSING','COMPLETED') else None
                result = result if state=='COMPLETED' else None
                changed = result or processing or collected or ordered
                yield self.row(created=ordered,updated=changed,lab_order_id=i,encounter_id=eid,department_id=dept,lab_test_type_id=test,status=state,status_changed_at=iso(changed),ordered_at=iso(ordered),collected_at=iso(collected) if collected else None,processing_started_at=iso(processing) if processing else None,result_completed_at=iso(result) if result else None,target_tat_minutes_at_order=target)
        elif table in ('encounter_doctors','encounter_diagnoses'):
            key = 0
            for eid,_,_,doctor,started,finished,_ in self.db.execute('SELECT * FROM contexts ORDER BY id'):
                if table=='encounter_doctors':
                    count = 2 if eid%5==1 else 1
                    for j in range(count+(1 if eid%97==0 else 0)):
                        key += 1
                        active = j<count
                        secondary = 1+(doctor-1+c.doctors//2+j-1)%c.doctors
                        yield self.row(created=moment(started),updated=moment(finished or started),association_id=key,encounter_id=eid,doctor_id=doctor if j==0 else secondary,role='PRIMARY' if j==0 else 'CONSULTING',joined_at=started,left_at=finished,is_active=active)
                else:
                    count = 3 if eid%5==1 else 1
                    for j in range(count+(1 if eid%89==0 else 0)):
                        key += 1
                        yield self.row(created=moment(started),association_id=key,encounter_id=eid,diagnosis_id=1+(eid+j)%c.diagnoses,is_primary=j==0,recorded_at=started,is_active=j<count)
        else:
            raise ValueError(table)

    def inpatients(self):
        n,c = self.counts,self.config
        parents = set()
        for i in range(1,n['inpatient_encounters']+1):
            proposed = self.slot(i,n['inpatient_encounters'],23)
            parent = None
            if i%5<3:
                for candidate in self.db.execute("SELECT id,patient,dept FROM contexts WHERE started<? AND kind='OUTPATIENT' ORDER BY started DESC LIMIT 100",(iso(proposed-timedelta(hours=1)),)):
                    if candidate[0] not in parents:
                        parent=candidate
                        parents.add(parent[0])
                        break
            dept = self.department(self.number(i,24))
            patient = parent[1] if parent else 1+self.number(i,25)%n['patients']
            self.db.execute('INSERT INTO requests VALUES(?,?,?,?,?)',(i,iso(proposed),parent[0] if parent else None,patient,dept))
        self.db.commit()
        available = {bed:self.start for bed in self.bed_depts}
        pools = {dept:[b for b,d in self.bed_depts.items() if d==dept] for dept in range(1,11)}
        assignment_id = 0

        def fit(bed,desired,duration):
            left = max(desired,available[bed])
            if bed in self.maintenance_beds and left<self.maintenance_end and left+duration>self.maintenance_start:
                left=self.maintenance_end
            return left

        for i,proposed,parent,patient,dept in self.db.execute('SELECT * FROM requests ORDER BY proposed,id'):
            duration = timedelta(days=c.mean_los_days*(.5+self.number(i,26)%1001/1000))
            options = [(fit(b,moment(proposed),duration),b) for b in pools[dept]]
            admitted,bed = min(options)
            if admitted>=self.end-timedelta(minutes=10):
                raise ValueError(f'Bed capacity exhausted in department {dept}; reconfigure capacity, never trim admissions')
            discharge = admitted+duration
            finished = discharge if discharge<self.end else None
            eid=n['outpatient_encounters']+i
            doctor=self.doctor(dept,i)
            self.db.execute('INSERT INTO contexts VALUES(?,?,?,?,?,?,?)',(eid,patient,dept,doctor,iso(admitted),iso(finished) if finished else None,'INPATIENT'))
            segments=[(bed,admitted,finished)]
            split=admitted+duration/2
            if i%5<2 and split<self.end:
                targets=[(fit(b,split,duration/2),b) for b in pools[dept] if b!=bed]
                target_time,target=min(targets) if targets else (self.end,None)
                if target is not None and target_time<=split:
                    segments=[(bed,admitted,split),(target,split,finished)]
            for selected,left,right in segments:
                assignment_id+=1
                available[selected]=(right or discharge)+timedelta(minutes=30)
                self.db.execute('INSERT INTO assignments VALUES(?,?,?,?,?)',(assignment_id,selected,eid,iso(left),iso(right) if right else None))
            yield self.row(created=admitted,updated=finished or admitted,encounter_id=eid,patient_id=patient,department_id=dept,doctor_id=doctor,appointment_id=None,parent_encounter_id=parent,encounter_type='INPATIENT',status='DISCHARGED' if finished else 'ADMITTED',status_changed_at=iso(finished or admitted),checked_in_at=None,consultation_started_at=None,consultation_completed_at=None,admitted_at=iso(admitted),discharged_at=iso(finished) if finished else None)
        self.db.commit()


def write_dataset(output, config=Config(), chunk_size=25000):
    """OS lock is released on crash; concurrent writers cannot recreate the FK index."""
    output=Path(output)
    output.mkdir(parents=True,exist_ok=True)
    lock=(output/'.generation.lock').open('a+b')
    try:
        try:
            if __import__('os').name=='nt':
                import msvcrt
                lock.seek(0)
                if not lock.read(1):
                    lock.write(b'0')
                    lock.flush()
                lock.seek(0)
                msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError as exc:
            raise ValueError('A generator already owns this output directory') from exc
        return _write_dataset(output,config,chunk_size)
    finally:
        lock.close()


def _write_dataset(output, config, chunk_size):
    if chunk_size<1:
        raise ValueError('chunk_size must be positive')
    output=Path(output)
    output.mkdir(parents=True,exist_ok=True)
    config_hash=hashlib.sha256(json.dumps(asdict(config),sort_keys=True).encode()).hexdigest()
    manifest_path=output/'manifest.json'
    if manifest_path.exists():
        previous=json.loads(manifest_path.read_text(encoding='utf-8'))
        if previous['config_sha256']!=config_hash:
            raise ValueError('Output contains a different frozen configuration; choose a new output directory')
    begun=time.monotonic()
    disk_before=shutil.disk_usage(output)
    # Random FK lookups against a Windows bind mount are very expensive in Docker.
    # This index is rebuildable scratch space, not part of the frozen dataset.
    # Keep it disk-backed on the runtime's local temporary filesystem.
    context_directory=tempfile.TemporaryDirectory(prefix='hospital-phase5-context-')
    workload=Workload(config,Path(context_directory.name)/'contexts.sqlite3')
    manifest={'generator_version':5,'synthetic_only':True,'config':asdict(config),'config_sha256':config_hash,
              'seed':config.seed,'as_of':iso(workload.end),'history_start':iso(workload.start),
              'horizon_days':config.horizon_days,'report_start_date':str(workload.start.date()),
              'report_end_date':str((workload.end-timedelta(microseconds=1)).date()),
              'encounters_per_day':config.encounters_per_day,'derived_counts':workload.counts,
              'counting_policy':'OP+IP encounters = average daily encounters * horizon; other rows derived from configured ratios',
              'context_index_storage':'disk-backed runtime-local temporary SQLite; 8 MiB SQLite page cache',
              'tables':{},'counts':{},'chunk_size':chunk_size}
    model_path=Path(__file__)
    manifest['model_sha256']=hashlib.sha256(model_path.read_bytes()).hexdigest()
    overall=hashlib.sha256()
    try:
        for table in LOAD_ORDER:
            folder=output/table
            folder.mkdir(exist_ok=True)
            columns=None
            chunk_rows=total=0
            chunks=[]
            table_hash=hashlib.sha256()
            handle=None
            chunk_hash=None
            chunk_no=0
            for row in workload.iter_table(table):
                if columns is None: columns=list(row)
                if not chunk_rows:
                    chunk_no+=1
                    relative=f'{table}/{chunk_no:05}.csv'
                    temp=output/(relative+'.part')
                    handle=temp.open('wb')
                    chunk_hash=hashlib.sha256()
                stream=io.StringIO(newline='')
                csv.writer(stream,lineterminator='\n').writerow(['\\N' if row[k] is None else row[k] for k in columns])
                encoded=stream.getvalue().encode('utf-8')
                handle.write(encoded)
                chunk_hash.update(encoded)
                table_hash.update(encoded)
                chunk_rows+=1
                total+=1
                if chunk_rows==chunk_size:
                    handle.close()
                    temp.replace(output/relative)
                    chunks.append({'path':relative,'rows':chunk_rows,'sha256':chunk_hash.hexdigest(),'bytes':(output/relative).stat().st_size})
                    chunk_rows=0
            if chunk_rows:
                handle.close()
                temp.replace(output/relative)
                chunks.append({'path':relative,'rows':chunk_rows,'sha256':chunk_hash.hexdigest(),'bytes':(output/relative).stat().st_size})
            manifest['tables'][table]={'columns':columns or [],'rows':total,'sha256':table_hash.hexdigest(),'chunks':chunks}
            manifest['counts'][table]=total
            overall.update(table.encode()+b'\0'+bytes.fromhex(table_hash.hexdigest()))
            print(json.dumps({'table':table,'rows':total,'elapsed_seconds':round(time.monotonic()-begun,2)}),flush=True)
        manifest['business_sha256']=overall.hexdigest()
        manifest['core_source_rows']=sum(manifest['counts'][t] for t in CORE_TABLES)
        manifest['association_rows']=manifest['counts']['encounter_doctors']+manifest['counts']['encounter_diagnoses']
        manifest['catalog_extension_rows']=manifest['counts']['diagnoses']
        manifest['oltp_total_rows']=sum(manifest['counts'].values())
        manifest['csv_bytes']=sum(ch['bytes'] for t in manifest['tables'].values() for ch in t['chunks'])
        manifest['generation_seconds']=round(time.monotonic()-begun,3)
        manifest['context_index_peak_disk_bytes']=sum(p.stat().st_size for p in Path(context_directory.name).iterdir() if p.is_file())
        manifest['disk_free_before_bytes']=disk_before.free
        manifest['disk_free_after_bytes']=shutil.disk_usage(output).free
        try:
            import resource
            manifest['peak_process_rss_bytes']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
        except ImportError:
            manifest['peak_process_rss_bytes']=None
        if manifest['counts']['encounters']!=config.encounters_per_day*config.horizon_days:
            raise ValueError('Derived encounter count reconciliation failed')
        temp=output/'manifest.json.part'
        temp.write_text(json.dumps(manifest,indent=2),encoding='utf-8')
        temp.replace(manifest_path)
        return manifest
    finally:
        workload.db.close()
        context_directory.cleanup()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=Path('artifacts/generated/phase5'))
    parser.add_argument('--encounters-per-day',type=int,default=6000)
    parser.add_argument('--horizon-days',type=int,default=180)
    parser.add_argument('--as-of',default=Config.as_of)
    parser.add_argument('--seed',type=int,default=Config.seed)
    parser.add_argument('--chunk-size',type=int,default=25000)
    args=parser.parse_args()
    result=write_dataset(args.output,Config(encounters_per_day=args.encounters_per_day,horizon_days=args.horizon_days,as_of=args.as_of,seed=args.seed),args.chunk_size)
    print(json.dumps({k:result[k] for k in ('core_source_rows','association_rows','catalog_extension_rows','oltp_total_rows','business_sha256')}))


if __name__=='__main__': main()
