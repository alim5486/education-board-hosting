(function () {
  'use strict';

  // EDIT: codes must match the exam_code values stored in the database
  // (they are sent as ?exam=CODE and must be 2-16 letters, digits, _ or -).
  var EXAMS = [
    ['ALIM', 'আলিম / Alim'],
    ['DAKHIL', 'দাখিল / Dakhil'],
    ['SSC', 'এসএসসি / SSC'],
    ['HSC', 'এইচএসসি / HSC']
  ];
  var YEARS_BACK = 5;

  // Same rules as the server; checking here saves a round trip for typos
  var RULES = {
    roll: { re: /^\d{4,12}$/, msg: 'রোল নম্বর ৪-১২ অঙ্কের হতে হবে / Roll must be 4-12 digits' },
    reg: { re: /^\d{4,15}$/, msg: 'রেজিস্ট্রেশন নম্বর ৪-১৫ অঙ্কের হতে হবে / Registration must be 4-15 digits' }
  };

  var STATUS = {
    PASSED: ['উত্তীর্ণ / Passed', 'pass'],
    FAILED: ['অনুত্তীর্ণ / Failed', 'fail'],
    WITHHELD: ['ফলাফল স্থগিত / Withheld', 'fail']
  };

  var $ = function (id) { return document.getElementById(id); };
  var form = $('f'), go = $('go'), msg = $('msg'), out = $('out');

  // ---- setup ----
  EXAMS.forEach(function (e) {
    var o = document.createElement('option');
    o.value = e[0];
    o.textContent = e[1];
    $('exam').appendChild(o);
  });
  var thisYear = new Date().getFullYear();
  for (var y = thisYear; y > thisYear - YEARS_BACK; y--) {
    var o = document.createElement('option');
    o.value = o.textContent = String(y);
    $('year').appendChild(o);
  }

  // Digits only, so a pasted "123 456" or Bengali digits don't cause avoidable errors
  var BN = '০১২৩৪৫৬৭৮৯';
  function digits(s) {
    return s.replace(/[০-৯]/g, function (d) { return String(BN.indexOf(d)); }).replace(/\D/g, '');
  }
  ['roll', 'reg'].forEach(function (id) {
    $(id).addEventListener('input', function (e) {
      var clean = digits(e.target.value);
      if (clean !== e.target.value) e.target.value = clean;
    });
  });

  // ---- helpers ----
  function el(tag, text, cls) {
    var n = document.createElement(tag);
    if (text !== undefined && text !== null) n.textContent = String(text); // never innerHTML
    if (cls) n.className = cls;
    return n;
  }

  function setMsg(text, isErr) {
    msg.textContent = text || '';
    msg.className = isErr ? 'err' : '';
  }

  function fieldError(id, text) {
    var input = $(id), note = $(id + '-err');
    if (text) { input.setAttribute('aria-invalid', 'true'); note.textContent = text; note.hidden = false; }
    else { input.removeAttribute('aria-invalid'); note.textContent = ''; note.hidden = true; }
  }

  function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

  // Retry only on 502/503/network errors, at most twice, honouring Retry-After and
  // adding jitter so a crowd of browsers does not retry in lockstep.
  async function lookup(params) {
    var url = '/api/results?' + new URLSearchParams(params).toString();
    var res = null;
    for (var attempt = 0; attempt < 3; attempt++) {
      try { res = await fetch(url, { headers: { accept: 'application/json' } }); }
      catch (e) { res = null; }
      if (res && res.status !== 502 && res.status !== 503) return res;
      if (attempt === 2) break;
      var ra = res ? parseInt(res.headers.get('retry-after'), 10) : NaN;
      var wait = (isNaN(ra) ? 3 : Math.min(ra, 10)) * 1000 + Math.random() * 2000;
      setMsg('সার্ভার ব্যস্ত, আবার চেষ্টা করা হচ্ছে… / Server busy, retrying…');
      await sleep(wait);
    }
    return res;
  }

  // ---- rendering ----
  function row(dl, label, value) {
    if (value === null || value === undefined || value === '') return;
    var d = el('div');
    d.appendChild(el('dt', label));
    d.appendChild(el('dd', value));
    dl.appendChild(d);
  }

  function subjectsView(subjects) {
    if (!Array.isArray(subjects) || subjects.length === 0) return null;
    var wrap = el('div', null, 'table-wrap');
    if (typeof subjects[0] === 'object' && subjects[0] !== null) {
      var cols = Object.keys(subjects[0]);
      var table = el('table');
      var thead = el('thead'), htr = el('tr');
      cols.forEach(function (c) { htr.appendChild(el('th', c.replace(/_/g, ' '))); });
      thead.appendChild(htr);
      table.appendChild(thead);
      var tbody = el('tbody');
      subjects.forEach(function (s) {
        var tr = el('tr');
        cols.forEach(function (c) { tr.appendChild(el('td', s[c])); });
        tbody.appendChild(tr);
      });
      table.appendChild(tbody);
      wrap.appendChild(table);
    } else {
      var ul = el('ul');
      subjects.forEach(function (s) { ul.appendChild(el('li', s)); });
      wrap.appendChild(ul);
    }
    return wrap;
  }

  function render(d) {
    out.textContent = '';
    var sheet = el('section', null, 'sheet');
    sheet.setAttribute('aria-label', 'Result');

    var head = el('div', null, 'sheet-head');
    var who = el('div', null, 'who');
    who.appendChild(el('strong', d.name));
    who.appendChild(el('span', (d.exam || '') + ' ' + (d.year || '')));
    head.appendChild(who);
    if (d.gpa !== null && d.gpa !== undefined) {
      var g = el('div', null, 'gpa');
      g.appendChild(el('b', Number(d.gpa).toFixed(2)));
      g.appendChild(el('span', 'জিপিএ / GPA' + (d.grade ? ' · ' + d.grade : '')));
      head.appendChild(g);
    }
    sheet.appendChild(head);

    var st = STATUS[d.status] || [d.status, ''];
    if (d.status) sheet.appendChild(el('div', st[0], 'status ' + st[1]));

    var dl = el('dl');
    row(dl, 'রোল / Roll', d.roll);
    row(dl, 'রেজিস্ট্রেশন / Registration', d.reg);
    row(dl, 'পিতার নাম / Father', d.father);
    row(dl, 'প্রতিষ্ঠান / Institution', d.institution);
    row(dl, 'বিভাগ / Group', d.group);
    sheet.appendChild(dl);

    var subs = subjectsView(d.subjects);
    if (subs) sheet.appendChild(subs);

    out.appendChild(sheet);
    sheet.scrollIntoView({ block: 'start' });
  }

  // ---- submit ----
  form.addEventListener('submit', async function (ev) {
    ev.preventDefault();
    out.textContent = '';
    setMsg('');

    var values = {
      exam: $('exam').value,
      year: $('year').value,
      roll: digits($('roll').value),
      reg: digits($('reg').value)
    };

    var bad = false;
    ['roll', 'reg'].forEach(function (k) {
      var ok = RULES[k].re.test(values[k]);
      fieldError(k, ok ? '' : RULES[k].msg);
      if (!ok) bad = true;
    });
    if (bad) { setMsg('তথ্য ঠিক করে আবার চেষ্টা করুন / Fix the highlighted fields', true); return; }

    go.disabled = true;
    setMsg('খোঁজা হচ্ছে… / Searching…');

    try {
      var res = await lookup(values);
      if (!res) {
        setMsg('সংযোগ করা যায়নি, কিছুক্ষণ পরে আবার চেষ্টা করুন / Could not connect. Try again shortly.', true);
      } else if (res.status === 200) {
        setMsg('');
        render(await res.json());
      } else if (res.status === 404) {
        setMsg('ফলাফল পাওয়া যায়নি। রোল ও রেজিস্ট্রেশন নম্বর মিলিয়ে দেখুন / No result found. Check your roll and registration numbers.', true);
      } else if (res.status === 400) {
        setMsg('তথ্য সঠিক নয়। পরীক্ষা, সাল, রোল ও রেজিস্ট্রেশন যাচাই করুন / Invalid details. Check exam, year, roll and registration.', true);
      } else if (res.status === 429) {
        var ra = parseInt(res.headers.get('retry-after'), 10);
        setMsg('অনেক বেশি অনুরোধ হয়েছে' + (isNaN(ra) ? '' : ', ' + ra + ' সেকেন্ড পরে') + ' আবার চেষ্টা করুন / Too many requests. Wait a moment and try again.', true);
      } else {
        setMsg('সার্ভার ব্যস্ত আছে, কিছুক্ষণ পরে আবার চেষ্টা করুন / Server is busy. Try again in a minute.', true);
      }
    } catch (e) {
      setMsg('কিছু ভুল হয়েছে, আবার চেষ্টা করুন / Something went wrong. Try again.', true);
    } finally {
      go.disabled = false;
    }
  });
})();
