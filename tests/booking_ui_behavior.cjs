const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const crypto = require('node:crypto');
const html = fs.readFileSync(require('node:path').join(__dirname,'../src/booking/index.html'),'utf8');
function harness(storage=new Map(),failOnce=false){
 class Element{
  constructor(){this.value='';this.hidden=false;this.disabled=false;this.children=[];this.handlers={};}
  addEventListener(name,handler){this.handlers[name]=handler;}
  append(...values){this.children.push(...values);}
  replaceChildren(){this.children=[];this.value='';}
  setCustomValidity(value){this.validationMessage=value;} setAttribute(name,value){this[name]=value;} reportValidity(){return !this.validationMessage;}
  focus(){} reset(){} showModal(){this.open=true;} close(){this.open=false;}
 }
 const elements=new Map(),el=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
 const posts=[];let receipt,fail=failOnce;
 const respond=(data,status=200)=>({ok:status<400,status,json:async()=>data});
 const context=vm.createContext({document:{getElementById:el,createElement:()=>new Element()},
  localStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value),removeItem:key=>storage.delete(key)},
  crypto:crypto.webcrypto,Date,Intl,AbortSignal,console,setInterval:()=>0,
  fetch:async(path,options={})=>{
   if(path==='/api/catalogs')return respond({server_now:new Date().toISOString(),default_slot_local:new Date(Date.now()+86400000).toISOString().slice(0,16),purposes:{GENERAL:'Khám sức khỏe tổng quát',FOLLOW_UP:'Tái khám theo lịch hẹn',SYMPTOMS:'Khám vì có triệu chứng',PERIODIC:'Khám sức khỏe định kỳ'},departments:[{id:1,name:'Nội tổng quát'}],doctors:[{id:1,name:'GS. TS. Nguyễn Minh An',department_id:1}]});
   if(path==='/api/registrations'&&options.method==='POST'){
    posts.push(JSON.parse(options.body));if(fail){fail=false;throw new TypeError('network failure');}
    receipt={...posts.at(-1),registration_saved:true,awaiting_patient_code:true,patient_id:null,appointment_id:null,encounter_id:null,status:'REGISTERED'};return respond(receipt);
   }
   if(path.startsWith('/api/registrations/'))return receipt?respond(receipt):respond({error:'Not found'},404);
   throw new Error('Unexpected '+path);
  }});
 vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1],context);
 return {el,posts,storage,setReceipt:value=>{receipt=value;},ready:()=>new Promise(setImmediate),act:(id,event='click')=>el(id).handlers[event]({preventDefault(){}})};
}
(async()=>{
 const h=harness(new Map(),true);await h.ready();
 h.el('purpose').value='FOLLOW_UP';h.act('purpose','change');assert.equal(h.el('patient-code').required,true);assert.equal(h.el('patient-code-field').hidden,false);
 h.el('purpose').value='GENERAL';h.act('purpose','change');assert.equal(h.el('patient-code-field').hidden,true);
 h.el('patient-name').value='Nguyễn Minh An';h.el('department').value='1';h.act('department','change');h.el('doctor').value='1';
 h.el('scheduled-at').value=new Date(Date.now()+86400000+7*3600000).toISOString().slice(0,16);
 h.act('booking-form','submit');assert.equal(h.posts.length,0,'preview never creates registration');assert.equal(h.el('confirmation').open,true);
 await h.act('confirm');assert.equal(h.posts.length,1);assert.equal(h.el('booking-fields').disabled,true,'ambiguous result locks payload');assert.equal(h.el('edit').disabled,true);
 await h.act('confirm');assert.deepEqual(h.posts[0],h.posts[1],'retry retains UUID and payload');assert.equal(h.el('receipt').hidden,false);assert.equal(h.el('confirmation').open,false);
 h.setReceipt({...h.posts[1],status:'NO_SHOW',appointment_id:99,encounter_id:null,awaiting_patient_code:true});await h.act('refresh');assert.equal(h.el('receipt-title').textContent,'Vắng hẹn · NO_SHOW');assert.equal(h.el('checkin').disabled,true);assert.match(h.el('receipt-note').textContent,/giữ lại/);
 h.act('new');assert.equal(h.storage.size,0);assert.equal(h.el('booking-fields').disabled,false);
 h.el('scheduled-at').value=new Date(Date.now()+7*3600000+10*60000).toISOString().slice(0,16);h.act('booking-form','submit');assert.match(h.el('slot-error').textContent,/30 phút/);assert.equal(h.el('scheduled-at')['aria-invalid'],'true');assert.equal(h.posts.length,2);
 h.el('scheduled-at').value=new Date(Date.now()+7*3600000+31*60000).toISOString().slice(0,16);h.act('scheduled-at','input');assert.equal(h.el('slot-error').hidden,true);assert.equal(h.el('scheduled-at')['aria-invalid'],'false');
 console.log('Booking UI checks PASS: follow-up field, preview, confirmation, retry isolation, new request, 30-minute validation.');
})().catch(error=>{console.error(error);process.exitCode=1;});