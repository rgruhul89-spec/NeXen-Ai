// Nexan Ai: single-file Cloudflare Worker (site + server).
// Nexan Ai v8 on Cloudflare Pages ("advanced mode" _worker.js). One file, no secrets inside.
// Set these in Cloudflare -> your Pages project -> Settings:
//   API_KEY (required), ADMIN_PASSWORD (for Teach Nexan), KV binding named KV (for Teach Nexan),
//   POLLINATIONS_KEY (optional), ACCESS_CODE (optional, locks chat/image behind a code), MODEL / IMAGE_MODEL (optional).
const J = (code, obj) =>
  new Response(JSON.stringify(obj), { status: code, headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' } });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const safeEq = (a, b) => { a = String(a); b = String(b); let d = a.length ^ b.length; for (let i = 0; i < Math.max(a.length, b.length); i++) d |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0); return d === 0; };
const hits = new Map(); // best-effort rate limit (per Cloudflare isolate)
const cool = new Map(); // model -> time until which it is skipped after hitting its free limit (429)
function limited(ip, bucket, max, ms) {
  const k = bucket + '|' + ip, n = Date.now(), a = (hits.get(k) || []).filter((t) => n - t < ms);
  if (a.length >= max) { hits.set(k, a); return true; }
  a.push(n); hits.set(k, a); if (hits.size > 5000) hits.clear();
  return false;
}
const NOKEY = 'API_KEY সেট করা নেই। Cloudflare → Settings → Variables-এ API_KEY বসান। / API_KEY is not set in Cloudflare.';

const BUSY = [429, 500, 502, 503, 504];
const BUSY_MSG = 'এখন সার্ভার ব্যস্ত, কয়েক সেকেন্ড পরে আবার পাঠান। / Server is busy, please try again in a few seconds.';

function buildSystem(context, project, voice) {
  return (
    'You are Nexan Ai, a friendly, helpful, modern AI assistant fluent in Bangla and English. ' +
    'Reply in the language the user writes in (Bangla or English). If they write Bangla in English letters, reply in Bangla script. ' +
    'You can understand images and screenshots the user uploads. You can talk about any topic: study, coding, writing, advice, general knowledge and more. Be clear and concise. ' +
    'If the user asks you to create, draw or generate an image, tell them to type it as: "draw <description>" or turn on the Image chip above the chat box. You can also read PDFs, documents, audio and video that the user attaches, and the user can turn on the Search and Code chips for web search and calculations. ' +
    'If you are not sure, say so instead of guessing.' +
    (project && project.name
      ? '\n\nYou are working inside the project "' + String(project.name).slice(0, 80) + '". The user wrote these project instructions:\n' +
        String(project.text || '').slice(0, 3000) +
        "\nStay focused on this project's topic and follow these instructions in every reply. If the user asks something clearly unrelated to the project, politely say this project is about its topic and offer to help with that."
      : '') +
    (context.length
      ? "\n\nOwner notes (written by the site owner, always correct). If the user's question is about something covered here, answer from these notes and repeat prices, dates, names and numbers exactly. Do not invent other details:\n" + context.join('\n')
      : '') +
    (voice
      ? '\n\nVoice conversation mode: your reply will be spoken aloud. Answer in 1 to 3 short, natural spoken sentences. No markdown, no lists, no emoji, no URLs, no code blocks. If a long answer is needed, give the key points and offer to continue.'
      : '')
  );
}

function cleanHistory(messages) {
  let history = messages.slice(-12).map((m) => ({
    role: m.role === 'user' ? 'user' : 'assistant',
    content: String(m.content || '').slice(0, 4000) || '.',
    images: Array.isArray(m.images)
      ? m.images
          .filter((i) => i && typeof i.data === 'string' && (/^(image|audio|video)\//.test(i.mime) || i.mime === 'application/pdf' || i.mime === 'text/plain') && i.data.length < 4000000)
          .slice(0, 4)
          .map((i) => ({ mime: i.mime, data: i.data }))
      : [],
  }));
  while (history.length && history[0].role !== 'user') history.shift();
  return history.reduce((a, m) => {
    const l = a[a.length - 1];
    if (l && l.role === m.role) { l.content += '\n' + m.content; l.images = l.images.concat(m.images); }
    else a.push({ ...m });
    return a;
  }, []);
}

const textOf = (data) => {
  const parts = (data && data.candidates && data.candidates[0] && data.candidates[0].content && data.candidates[0].content.parts) || [];
  return parts.map((p) => p.text || '').join('');
};

// Open a Gemini stream. Resolves { reader, firstText } as soon as the first text arrives, so a bad/busy model can be skipped.
async function openGemini(model, key, body, ms) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), ms);
  let res;
  try {
    res = await fetch(
      'https://generativelanguage.googleapis.com/v1beta/models/' + model + ':streamGenerateContent?alt=sse',
      { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-goog-api-key': key }, body, signal: ac.signal }
    );
  } catch (e) {
    clearTimeout(timer);
    return { err: { code: 504, msg: 'timeout' } };
  }
  if (!res.ok) {
    clearTimeout(timer);
    const d = await res.json().catch(() => ({}));
    const e0 = Array.isArray(d) ? d[0] && d[0].error : d.error;
    return { err: { code: res.status, msg: (e0 && e0.message) || 'API error' } };
  }
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  let src = [];
  // Async generator: yields text pieces from the SSE stream.
  async function* pieces(first) {
    if (first) yield first;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let i;
      while ((i = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, i).trim();
        buf = buf.slice(i + 1);
        if (!line.startsWith('data:')) continue;
        try {
          const j = JSON.parse(line.slice(5));
          const t = textOf(j);
          const gm = j.candidates && j.candidates[0] && j.candidates[0].groundingMetadata;
          if (gm && Array.isArray(gm.groundingChunks)) src = gm.groundingChunks.map((c) => c.web && c.web.title).filter(Boolean);
          if (t) yield t;
        } catch (e) { /* partial line */ }
      }
    }
    if (src.length) yield '\n\nSources: ' + [...new Set(src)].slice(0, 5).join(', ');
  }
  const gen = pieces(null);
  const first = await gen.next();
  clearTimeout(timer);
  if (first.done) return { err: { code: 502, msg: 'Empty reply, please rephrase and try again.' } };
  return { first: first.value, gen };
}


async function cfChat(env, system, history, voice) {
  try {
    const o = await env.AI.run('@cf/google/gemma-4-26b-a4b-it', {
      messages: [{ role: 'system', content: system }, ...history.map((m) => ({ role: m.role, content: m.content }))],
      max_tokens: voice ? 300 : 1024,
      temperature: 0.7,
    });
    const t = o && (o.response || (o.choices && o.choices[0] && o.choices[0].message && o.choices[0].message.content));
    return typeof t === 'string' ? t.trim() : '';
  } catch (e) { return ''; }
}

async function chat(request, env, ctx) {
  const key = env.API_KEY;
  if (!key) return J(500, { error: NOKEY });
  const body0 = await request.json().catch(() => ({}));
  const { messages = [], project = null, search = false, code = false, voice = false } = body0;
  const context = (Array.isArray(body0.context) ? body0.context : []).slice(0, 8).map((s) => String(s).slice(0, 4000));
  const system = buildSystem(context, project, !!voice);
  const history = cleanHistory(Array.isArray(messages) ? messages : []);
  if (!history.length) return J(400, { error: 'Empty message' });

  // Gemini (AQ.… or AIza… keys): streaming + fallback models. Groq (gsk_…) goes to the OpenAI-style branch below.
  if (!key.startsWith('gsk_') && !env.API_URL) {
    const models = [env.MODEL || 'gemini-3.8-flash', 'gemini-flash-lite-latest', 'gemini-flash-latest'];
    const base = {
      systemInstruction: { parts: [{ text: system }] },
      contents: history.map((m) => ({
        role: m.role === 'user' ? 'user' : 'model',
        parts: [...m.images.map((i) => ({ inlineData: { mimeType: i.mime, data: i.data } })), { text: m.content }],
      })),
      generationConfig: { temperature: 0.7, maxOutputTokens: voice ? 400 : 2048 },
    };
    const lastUser = [...history].reverse().find((m) => m.role === 'user');
    const tools = [];
    if (search) tools.push({ googleSearch: {} });
    if (code) tools.push({ codeExecution: {} });
    if (lastUser && /https?:\/\/\S+/i.test(lastUser.content)) tools.push({ urlContext: {} });
    const variants = [];
    if (tools.length) { variants.push(JSON.stringify({ ...base, tools })); if (tools.length > 1) variants.push(JSON.stringify({ ...base, tools: [tools[0]] })); }
    variants.push(JSON.stringify(base));
    const t0 = Date.now();
    let err = null;
    for (let round = 0; round < 2; round++) {
      for (const model of models) {
        if ((cool.get(model) || 0) > Date.now() && model !== models[models.length - 1]) continue;
        const left = 40000 - (Date.now() - t0);
        if (left < 3000) break;
        const ms = (tools.length || model !== models[0]) ? left : Math.min(left, 9000);
        let r;
        for (const v of variants) {
          r = await openGemini(model, key, v, ms);
          if (!(r.err && r.err.code === 400)) break;
        }
        if (r.err) {
          err = r.err;
          if (err.code === 429) cool.set(model, Date.now() + 120000);
          if (!BUSY.includes(err.code) && err.code !== 404) return J(err.code, { error: err.msg });
          continue;
        }
        const enc = new TextEncoder();
        const { readable, writable } = new TransformStream();
        const w = writable.getWriter();
        ctx.waitUntil((async () => {
          try { await w.write(enc.encode(r.first)); for await (const t of r.gen) await w.write(enc.encode(t)); } catch (e) { /* keep what was sent */ }
          try { await w.close(); } catch (e) { /* closed */ }
        })());
        return new Response(readable, { headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no' } });
      }
      if (Date.now() - t0 > 40000) break;
      await sleep(800);
    }
    // Every Gemini model is busy or out of free quota: answer with Cloudflare's free Workers AI instead of failing.
    if (env.AI) {
      const txt = await cfChat(env, system, history, !!voice);
      if (txt) return new Response(txt, { headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' } });
    }
    return J((err && err.code) || 503, { error: BUSY_MSG });
  }

  // OpenAI-compatible endpoint (Groq for gsk_ keys, or your own API_URL). Not streamed.
  const url = env.API_URL || 'https://api.groq.com/openai/v1/chat/completions';
  const model = env.MODEL || 'llama-3.3-70b-versatile';
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + key },
    body: JSON.stringify({
      model,
      messages: [{ role: 'system', content: system }, ...history.map((m) => ({ role: m.role, content: m.content }))],
      temperature: 0.7,
      max_tokens: voice ? 300 : 1024,
    }),
  });
  const data = await res.json().catch(() => ({}));
  const apiErr = Array.isArray(data) ? data[0] && data[0].error : data.error;
  if (!res.ok) return J(res.status, { error: (apiErr && apiErr.message) || 'API error' });
  const reply = (data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content) || '';
  if (!reply) return J(502, { error: 'Empty reply, please rephrase and try again.' });
  return new Response(reply, { headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' } });
}

async function gem(model, key, payload, ms) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), ms);
  try {
    const res = await fetch('https://generativelanguage.googleapis.com/v1beta/models/' + model + ':generateContent', {
      method: 'POST', headers: { 'Content-Type': 'application/json', 'x-goog-api-key': key }, body: JSON.stringify(payload), signal: ac.signal,
    });
    return { ok: res.ok, status: res.status, data: await res.json().catch(() => ({})) };
  } catch (e) { return { ok: false, status: 504, data: {} }; } finally { clearTimeout(timer); }
}

// Turn the user's image request into one rich English prompt (FLUX gives far better pictures from detailed English).
// Gemini Flash-Lite does it; Cloudflare's free translator is the backup for Bangla.
const PROMPT_RULES =
  'You write prompts for a text-to-image model (FLUX). Rewrite the user\'s request as ONE detailed English prompt of 40 to 90 words. ' +
  'Keep the subject, style and every detail the user asked for (cartoon, anime, logo, painting, 3D, photo and so on). ' +
  'If no style is given, choose a realistic, sharp, high-detail photograph or polished illustration that suits the subject. ' +
  'Add concrete visual detail: composition, lighting, colors, camera angle or art medium, mood. ' +
  'Do not put any text, letters, captions or watermarks in the image unless the user asked for specific text (then quote that exact text). ' +
  'Reply with the prompt only, no quotes, no explanation.';
async function buildPrompt(prompt, env, key) {
  const nonEn = /[^\x00-\x7F]/.test(prompt);
  if (!nonEn && prompt.length > 220) return prompt; // already detailed
  const t = await gem('gemini-flash-lite-latest', key, {
    systemInstruction: { parts: [{ text: PROMPT_RULES }] },
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
    generationConfig: { temperature: 0.6, maxOutputTokens: 400 },
  }, 7000);
  const txt = t.ok && t.data.candidates && t.data.candidates[0] && t.data.candidates[0].content && t.data.candidates[0].content.parts
    ? t.data.candidates[0].content.parts.map((p) => p.text || '').join('').trim() : '';
  if (txt) return txt.slice(0, 900);
  if (nonEn && env.AI) {
    try {
      const o = await env.AI.run('@cf/meta/m2m100-1.2b', { text: prompt.slice(0, 500), source_lang: 'bengali', target_lang: 'english' });
      if (o && o.translated_text) return String(o.translated_text).slice(0, 500);
    } catch (e) { /* keep original */ }
  }
  return prompt;
}

// Cloudflare Workers AI image models, best first. CF_IMAGE_MODEL (optional) replaces the first one,
// e.g. @cf/black-forest-labs/flux-2-klein-9b for the best quality (uses the free daily quota much faster).
async function cfImage(env, prompt) {
  const models = [env.CF_IMAGE_MODEL || '@cf/black-forest-labs/flux-2-klein-4b', '@cf/black-forest-labs/flux-1-schnell'];
  for (const model of models) {
    try {
      let out;
      if (model.includes('flux-1-schnell')) {
        out = await env.AI.run(model, { prompt: prompt.slice(0, 2000), steps: 6 });
      } else { // FLUX.2 models take multipart form data, even for a plain prompt
        const form = new FormData();
        form.append('prompt', prompt);
        form.append('width', '1024');
        form.append('height', '1024');
        const fr = new Response(form);
        out = await env.AI.run(model, { multipart: { body: fr.body, contentType: fr.headers.get('content-type') } });
      }
      const b64 = out && typeof out.image === 'string' ? out.image : '';
      if (b64.length > 1000) return { b64, mime: b64.startsWith('iVBOR') ? 'image/png' : 'image/jpeg', model };
    } catch (e) { /* daily free quota used up or model busy: try the next one */ }
  }
  return null;
}

async function image(request, env) {
  const key = env.API_KEY;
  if (!key) return J(500, { error: NOKEY });
  const b = await request.json().catch(() => ({}));
  const prompt = String(b.prompt || '').trim().slice(0, 1000);
  if (!prompt) return J(400, { error: 'Empty prompt' });
  const imgs = (Array.isArray(b.images) ? b.images : []).filter((i) => i && typeof i.data === 'string' && /^image\//.test(i.mime) && i.data.length < 4000000).slice(0, 3);
  const t0 = Date.now();
  const pk = env.POLLINATIONS_KEY || '';
  const model = env.IMAGE_MODEL || 'gemini-2.5-flash-image';

  // 0) Cloudflare Workers AI (FLUX.2 klein, then FLUX.1 schnell). Free daily allowance, no API key: it uses the AI binding. Skipped for image edits.
  let english = null;
  if (env.AI && !imgs.length && !env.IMAGE_MODEL) {
    english = await buildPrompt(prompt, env, key);
    const g = await cfImage(env, english);
    if (g) {
      // built by hand: no JSON.stringify pass over a ~1 MB string (saves CPU time on the free plan)
      return new Response('{"image":"data:' + g.mime + ';base64,' + g.b64 + '","via":"workers-ai"}', {
        status: 200, headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'X-Nexa-Via': g.model },
      });
    }
  }

  // 1) Gemini image model (also the only one that can edit an uploaded image). Skipped when a Pollinations key exists and nothing is uploaded.
  const r = (pk && !env.IMAGE_MODEL && !imgs.length) ? { ok: false } : await gem(model, key, {
    contents: [{ role: 'user', parts: [...imgs.map((i) => ({ inlineData: { mimeType: i.mime, data: i.data } })), { text: (imgs.length ? 'Edit this image as requested: ' : 'Generate an image: ') + prompt }] }],
    generationConfig: { responseModalities: ['TEXT', 'IMAGE'] },
  }, 25000);
  if (r.ok) {
    const parts = (r.data.candidates && r.data.candidates[0] && r.data.candidates[0].content && r.data.candidates[0].content.parts) || [];
    const img = parts.find((p) => p.inlineData || p.inline_data);
    const d = img && (img.inlineData || img.inline_data);
    if (d && d.data) return J(200, { image: 'data:' + (d.mimeType || d.mime_type || 'image/png') + ';base64,' + d.data, via: 'gemini' });
  }
  if (imgs.length) return J(200, { editFailed: true });

  // 2) English prompt for the free image service
  if (english === null) english = await buildPrompt(prompt, env, key);

  // 3) Pollinations with your own key. The image bytes are passed straight through (no base64 work on the server).
  if (pk) {
    const left = 45000 - (Date.now() - t0);
    if (left > 3000) {
      const ac = new AbortController();
      const timer = setTimeout(() => ac.abort(), left);
      try {
        const res = await fetch('https://gen.pollinations.ai/image/' + encodeURIComponent(english) +
          '?model=flux&width=1024&height=1024&nologo=true&seed=' + Math.floor(Math.random() * 1e6),
          { headers: { Authorization: 'Bearer ' + pk }, signal: ac.signal });
        const type = res.headers.get('content-type') || '';
        if (res.ok && type.startsWith('image/')) {
          return new Response(res.body, { headers: { 'Content-Type': type.split(';')[0], 'Cache-Control': 'no-store', 'X-Nexa-Via': 'pollinations-key' } });
        }
      } catch (e) { /* fall through */ } finally { clearTimeout(timer); }
    }
  }

  // 4) Last resort: browser uses the keyless service (limited to 1 image per 45 seconds)
  return J(200, { fallback: true, prompt: english });
}

async function kb(request, env, ip) {
  const list = env.KV ? ((await env.KV.get('entries', { type: 'json' })) || []) : [];
  if (request.method === 'GET') return J(200, { entries: list });
  if (request.method !== 'POST') return J(405, { error: 'Method not allowed' });
  if (limited(ip, 'kb', 20, 60000)) return J(429, { error: 'Too many attempts, wait a minute' });
  const b = await request.json().catch(() => ({}));
  if (!env.ADMIN_PASSWORD) return J(500, { error: 'ADMIN_PASSWORD is not set in Cloudflare' });
  if (!b.password || !safeEq(b.password, env.ADMIN_PASSWORD)) return J(401, { error: 'Wrong password' });
  if (b.action === 'check') return J(200, { ok: true });
  if (!env.KV) return J(500, { error: 'KV is not set up. Add a KV binding named KV (see README).' });
  if (b.action === 'add') {
    const keys = (Array.isArray(b.keys) ? b.keys : []).map((k) => String(k).trim().slice(0, 60)).filter(Boolean).slice(0, 30);
    const text = String(b.text || '').trim().slice(0, 4000);
    if (!keys.length || !text) return J(400, { error: 'Keywords and info are required' });
    if (list.length >= 300) return J(400, { error: 'Limit reached (300 entries)' });
    const entry = { id: 'g_' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6), u: 1, g: 1, keys, text };
    list.push(entry);
    await env.KV.put('entries', JSON.stringify(list));
    return J(200, { entry });
  }
  if (b.action === 'delete') {
    await env.KV.put('entries', JSON.stringify(list.filter((e) => e.id !== b.id)));
    return J(200, { ok: true });
  }
  return J(400, { error: 'Unknown action' });
}

const KB_JSON = `[
 {"id":"capital","keys":["capital","dhaka","রাজধানী","ঢাকা"],"text":"Dhaka is the capital of Bangladesh. ঢাকা বাংলাদেশের রাজধানী।"},
 {"id":"divisions","keys":["division","divisions","বিভাগ"],"text":"Bangladesh has 8 divisions: Dhaka, Chattogram, Rajshahi, Khulna, Barishal, Sylhet, Rangpur, Mymensingh. বাংলাদেশে ৮টি বিভাগ: ঢাকা, চট্টগ্রাম, রাজশাহী, খুলনা, বরিশাল, সিলেট, রংপুর, ময়মনসিংহ।"},
 {"id":"symbols","keys":["national","symbol","flower","bird","fruit","animal","fish","জাতীয়","ফুল","পাখি","ফল","পশু","মাছ"],"text":"National symbols: flower shapla (water lily), bird doyel (magpie-robin), fruit kanthal (jackfruit), animal Royal Bengal tiger, fish ilish (hilsa). জাতীয় ফুল শাপলা, পাখি দোয়েল, ফল কাঁঠাল, পশু রয়েল বেঙ্গল টাইগার, মাছ ইলিশ।"},
 {"id":"independence","keys":["independence","liberation","victory","স্বাধীনতা","মুক্তিযুদ্ধ","বিজয়","1971","১৯৭১"],"text":"Independence Day is 26 March and Victory Day is 16 December (1971). স্বাধীনতা দিবস ২৬ মার্চ, বিজয় দিবস ১৬ ডিসেম্বর।"},
 {"id":"language","keys":["language","mother","february","ভাষা","একুশে","ফেব্রুয়ারি","শহীদ"],"text":"21 February is Language Movement Day. UNESCO proclaimed it International Mother Language Day in 1999. ২১ ফেব্রুয়ারি শহীদ দিবস ও আন্তর্জাতিক মাতৃভাষা দিবস।"},
 {"id":"anthem","keys":["anthem","tagore","সংগীত","রবীন্দ্রনাথ","ঠাকুর"],"text":"The national anthem is 'Amar Shonar Bangla', written by Rabindranath Tagore. জাতীয় সংগীত 'আমার সোনার বাংলা', রচয়িতা রবীন্দ্রনাথ ঠাকুর।"},
 {"id":"currency","keys":["currency","taka","money","মুদ্রা","টাকা"],"text":"The currency of Bangladesh is the taka (BDT). বাংলাদেশের মুদ্রা টাকা।"},
 {"id":"places","keys":["sundarbans","cox","beach","tourist","সুন্দরবন","কক্সবাজার","সমুদ্র","সৈকত"],"text":"Cox's Bazar has one of the longest natural sea beaches in the world. The Sundarbans is the largest mangrove forest in the world and home to the Royal Bengal tiger. কক্সবাজার সমুদ্রসৈকত ও সুন্দরবন বিখ্যাত।"}
]
`;
const INDEX_HTML = `<!DOCTYPE html>
<html lang="bn">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Nexan Ai · By Ruhul</title>
<meta name="author" content="Ruhul">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%2305070D'/%3E%3Cpath d='M8 24V8l16 16V12' fill='none' stroke='%232F6BFF' stroke-width='3.4' stroke-linecap='round' stroke-linejoin='round'/%3E%3Ccircle cx='24' cy='7' r='2.2' fill='%239CC0FF'/%3E%3C/svg%3E">
<style>
:root{--bg:#05070D;--side:#080C16;--panel:#0E1626;--line:#1A2744;--blue:#2F6BFF;--lb:#6FA3FF;--txt:#E8EEFB;--mut:#8A9BBE}
*{box-sizing:border-box;margin:0;scrollbar-width:thin;scrollbar-color:#1A2744 transparent}
html,body{height:100%}
body{font-family:system-ui,-apple-system,'Segoe UI',Roboto,'Noto Sans Bengali','Hind Siliguri',sans-serif;background:var(--bg);color:var(--txt);display:flex;overflow:hidden;height:100vh;height:100dvh}
button,textarea,input{font:inherit;color:inherit}
:focus-visible{outline:2px solid var(--lb);outline-offset:2px}
aside{width:272px;background:#080C16;padding:14px;display:flex;flex-direction:column;gap:10px;border-right:1px solid var(--line);z-index:5;transition:transform .3s}
.brand{display:flex;align-items:center;gap:10px;font-weight:600;font-size:19px;padding:4px 6px}
.brand svg{width:30px;height:30px}
.brand #sx{margin-left:auto;display:none;place-items:center;width:40px;height:40px;border-radius:50%;background:var(--panel);border:1px solid var(--line);color:var(--txt);cursor:pointer}
.brand #sx svg{width:20px;height:20px}
.sb{display:flex;align-items:center;gap:12px;background:#0C1424;border:1px solid var(--line);border-radius:14px;padding:9px 12px;cursor:pointer;text-align:left;color:var(--txt);transition:background .2s,border-color .2s,box-shadow .2s,transform .2s}
.sb:hover{border-color:var(--blue);box-shadow:0 6px 22px #2F6BFF33;transform:translateY(-1px)}
.sb:active{transform:scale(.98)}
.sb .lb{display:flex;flex-direction:column;line-height:1.3}.sb .lb b{font-weight:600;font-size:14.5px}.sb .lb small{color:var(--mut);font-size:12.5px}
.sb.pri{background:linear-gradient(135deg,#2F6BFF,#1B3FD1);border:0;box-shadow:0 8px 24px #2F6BFF40}
.sb.pri .lb small{color:#CFE0FF}.sb.pri .ic{background:#ffffff26}
.sec{font-size:12.5px;color:var(--mut);padding:6px 8px 0}
.ic{width:34px;height:34px;border-radius:11px;display:grid;place-items:center;background:linear-gradient(135deg,#1B3FD1,#2F6BFF);color:#fff;flex:none}
.sb:not(.pri) .ic{background:#12203F;color:var(--lb)}
#list{flex:1;overflow:auto;display:flex;flex-direction:column;gap:2px}
.it{display:flex;align-items:center;gap:6px;padding:9px 10px;border-radius:10px;cursor:pointer;color:var(--mut);font-size:14.5px;transition:background .2s,transform .2s}
.it:hover,.it.on{background:var(--panel);color:var(--txt)}.it.on{box-shadow:inset 3px 0 0 var(--blue)}.it:hover{transform:translateX(3px)}
.it span{flex:1;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
.it button{background:none;border:0;cursor:pointer;color:var(--mut);opacity:0;padding:2px 6px}
.it:hover button{opacity:1}
.empty{color:var(--mut);font-size:13.5px;padding:8px;line-height:1.5}
main{flex:1;display:flex;flex-direction:column;min-width:0;position:relative;overflow:hidden}
main::before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(620px 420px at 18% 8%,#2F6BFF30,transparent 60%),radial-gradient(520px 420px at 88% 92%,#1B3FD140,transparent 60%)}
.bar,#msgs,.foot{position:relative;z-index:1}
.bar{display:none;padding:10px 14px;align-items:center;gap:10px}
.bar .rb{flex:none;width:44px;height:44px;border-radius:50%;background:var(--panel);border:1px solid var(--line);display:grid;place-items:center;cursor:pointer;color:var(--txt);transition:transform .2s,border-color .2s}
.bar .rb:hover{border-color:var(--blue)}.bar .rb:active{transform:scale(.92)}
.bar b{flex:1;text-align:center;font-weight:600;font-size:16px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
#msgs{flex:1;overflow:auto;padding:24px 16px}
#wel{position:absolute;z-index:1;inset:0 0 130px;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:10px;text-align:center;padding:16px;pointer-events:none}
.big{width:84px;height:84px;margin-bottom:6px}
.dr{stroke-dasharray:1;animation:draw 1.4s cubic-bezier(.6,0,.2,1) both}
.sp{animation:pop .5s 1.2s both}
@keyframes draw{from{stroke-dashoffset:1}}
@keyframes pop{from{opacity:0;transform:scale(0)}}
@keyframes glow{50%{filter:drop-shadow(0 0 14px #2F6BFF99)}}
#wel h1{font-size:28px;font-weight:600;line-height:1.35;color:var(--txt);animation:up .5s both}
#wel h1 small{display:block;font-size:.82em;font-weight:500}
#wel p{color:var(--mut);animation:up .8s .5s both;line-height:1.6}
@keyframes up{from{opacity:0;transform:translateY(16px)}}
@keyframes sh{to{background-position:-200% 0}}
.m{max-width:768px;margin:0 auto 22px;display:flex;gap:14px;animation:fade .18s both}
@keyframes fade{from{opacity:0}}
@keyframes blink{50%{opacity:.45}}
@keyframes pop2{from{opacity:0;transform:translateY(16px) scale(.97)}}
.m.u{justify-content:flex-end}
.av{flex:none;width:30px;height:30px;border-radius:50%;background:var(--panel);border:1px solid var(--line);display:grid;place-items:center}
.av svg{width:18px;height:18px}
.u .av{display:none}
.col{min-width:0}.u .col{display:flex;flex-direction:column;align-items:flex-end}
.b{line-height:1.75;white-space:pre-wrap;overflow-wrap:anywhere}
.u .b{background:#12203F;border-radius:20px;padding:10px 16px;max-width:560px}
.b.err{color:#FF8A9A}
.b code{background:#0E1626;border:1px solid var(--line);border-radius:6px;padding:1px 6px;font-size:.92em}
.b pre{background:#0A1020;border:1px solid var(--line);border-radius:12px;padding:12px 14px;margin:8px 0;overflow-x:auto}
.b pre code{background:none;border:0;padding:0}
.imgs{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end;margin-bottom:6px}
.imgs img{max-width:220px;max-height:180px;border-radius:14px;border:1px solid var(--line);display:block}
.cp{background:none;border:0;color:var(--mut);cursor:pointer;font-size:13px;margin-top:6px;padding:2px 0}
.acts{display:flex;gap:2px;margin-top:6px;margin-left:-8px}
.acts button{width:36px;height:36px;border-radius:50%;background:none;border:0;color:var(--mut);cursor:pointer;display:grid;place-items:center;transition:background .2s,color .2s,transform .15s}
.acts button:hover{background:#12203F;color:var(--lb)}.acts button:active{transform:scale(.88)}.acts button.ok{color:#5EE6A0}
.cp:hover{color:var(--lb)}
.dots{display:inline-flex;align-items:center;gap:3px;height:22px;vertical-align:middle}
.dots span{width:4px;height:100%;border-radius:4px;background:linear-gradient(#9CC0FF,#2F6BFF);animation:wave 1.1s ease-in-out infinite}
.dots span:nth-child(2){animation-delay:.12s}.dots span:nth-child(3){animation-delay:.24s}.dots span:nth-child(4){animation-delay:.36s}.dots span:nth-child(5){animation-delay:.48s}
@keyframes wave{0%,100%{transform:scaleY(.25);opacity:.5}50%{transform:scaleY(1);opacity:1}}
.av{position:relative}
.av.think{animation:blink 1.2s ease-in-out infinite}
.dots::after{content:"Thinking… · ভাবছে…";margin-left:8px;color:var(--mut)}
.foot{max-width:768px;width:100%;margin:0 auto;padding:8px 16px 18px}
#strip{display:flex;gap:12px;flex-wrap:wrap;padding:10px 10px 10px}
#strip:empty{display:none}
.th{position:relative;width:72px;height:72px;animation:pop2 .3s both}
.th img{width:100%;height:100%;object-fit:cover;border-radius:14px;border:1px solid var(--line);display:block}
.th button{position:absolute;top:-9px;right:-9px;width:26px;height:26px;border-radius:50%;border:2px solid var(--bg);background:#fff;color:#05070D;cursor:pointer;font-size:12px;font-weight:700;line-height:1;display:grid;place-items:center;box-shadow:0 2px 8px #000A;transition:transform .15s,background .15s,color .15s}
.th button:hover{background:#FF5C73;color:#fff;transform:scale(1.12)}
form{display:flex;align-items:flex-end;gap:6px;border-radius:28px;padding:8px 8px 8px 10px;border:1px solid var(--line);background:var(--panel);transition:border-color .2s}
form:focus-within{border-color:var(--blue)}
#in{flex:1;resize:none;max-height:160px;background:none;border:0;outline:0;padding:9px 4px;line-height:1.5}
#in::placeholder{color:var(--mut)}
#at{flex:none;width:38px;height:38px;border-radius:50%;background:none;border:0;cursor:pointer;display:grid;place-items:center;color:var(--mut);transition:transform .3s,color .2s,background .2s}
#at:hover{color:var(--lb);background:#12203F;transform:rotate(-20deg) scale(1.1)}
#go{flex:none;width:38px;height:38px;border-radius:50%;background:#1A2744;border:0;cursor:pointer;display:grid;place-items:center;opacity:.55;transform:scale(.88);transition:transform .25s cubic-bezier(.3,1.5,.5,1),opacity .25s,background .25s}
.ready #go{background:var(--blue);opacity:1;transform:scale(1)}
.ready #go:hover{transform:scale(1.12)}#go:active{transform:scale(.9)}
#go:disabled{cursor:default}
@keyframes ring{0%{box-shadow:0 0 0 0 #2F6BFF77}70%,100%{box-shadow:0 0 0 12px #2F6BFF00}}
.gimg{display:block;width:min(100%,420px);aspect-ratio:1/1;object-fit:cover;border-radius:16px;border:1px solid var(--line);background:#0E1626;animation:up .5s both}
.gsk{width:min(100%,420px);aspect-ratio:1/1;border-radius:16px;border:1px solid var(--line);display:grid;place-items:center;text-align:center;padding:16px;line-height:1.6;color:var(--mut);background:#0E1626;animation:blink 1.4s ease-in-out infinite}
.dl{display:inline-block;text-decoration:none;margin-right:14px}
#drop{position:fixed;inset:12px;border:2px dashed var(--blue);border-radius:24px;background:#05070DE6;display:none;place-items:center;font-size:20px;z-index:30;pointer-events:none;text-align:center;line-height:1.6}
#drop.on{display:grid}
dialog{margin:auto;padding:0;color:var(--txt);width:min(560px,94vw);max-height:92vh;overflow:auto;border:1px solid var(--line);border-radius:24px;background:linear-gradient(180deg,#0B1322,#070B14);box-shadow:0 30px 80px #000A}
dialog[open]{animation:pop2 .35s cubic-bezier(.2,.9,.3,1.15) both}
dialog::backdrop{background:#000B}
.dh{display:flex;align-items:center;gap:14px;padding:20px 22px 6px}
.dh h2{font-size:20px;line-height:1.2}.dh .sub{color:var(--mut);font-size:14px}
.dh .ic{width:42px;height:42px;border-radius:14px;box-shadow:0 8px 20px #2F6BFF40}
.x{margin-left:auto;background:none;border:0;color:var(--mut);cursor:pointer;width:34px;height:34px;border-radius:50%;transition:all .25s}
.x:hover{background:#12203F;color:#fff;transform:rotate(90deg)}
.hint{padding:6px 22px 14px;color:var(--mut);font-size:13.5px;line-height:1.65}
.card{margin:0 18px;padding:16px;background:#0C1424;border:1px solid var(--line);border-radius:18px;display:flex;flex-direction:column;gap:8px}
.card label{font-size:13.5px;font-weight:500;display:flex;gap:6px;align-items:baseline}.card label small{color:var(--mut);font-weight:400}
.chipbox{display:flex;flex-wrap:wrap;gap:6px;align-items:center;background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:8px 10px;cursor:text;transition:border-color .2s,box-shadow .2s}
.chipbox:focus-within,.fld:focus{border-color:var(--blue);box-shadow:0 0 0 3px #2F6BFF26}
#chips{display:contents}
.chipbox input{flex:1;min-width:150px;background:none;border:0;outline:0;padding:3px 2px}
.chip{display:inline-flex;align-items:center;gap:4px;background:#12275A;color:#BFD3FF;border-radius:20px;padding:3px 6px 3px 11px;font-size:13px;animation:pop2 .25s both}
.chip.s{padding:3px 11px}
.chip button{background:none;border:0;color:#8FA8E0;cursor:pointer;font-size:11px;padding:0 3px}
.eg{color:var(--mut);font-size:12.5px;margin:-2px 0 4px}
.fld{width:100%;background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:10px 12px;outline:0;resize:vertical;line-height:1.6;transition:border-color .2s,box-shadow .2s}
.btn{border:0;border-radius:12px;padding:11px;cursor:pointer;font-weight:500;background:linear-gradient(135deg,#2F6BFF,#1B3FD1);box-shadow:0 8px 24px #2F6BFF40;transition:transform .15s}
.btn:hover{transform:translateY(-1px) scale(1.01)}.btn:active{transform:scale(.97)}
.pw{display:flex;gap:8px}.pw .fld{flex:1;resize:none;min-width:0}.pw .btn{padding:0 16px;white-space:nowrap}
#tform{display:none;flex-direction:column;gap:8px}
#tmsg{min-height:1em;margin:0;line-height:1.5}
.pub{background:#12275A;color:#BFD3FF;border:0;border-radius:8px;padding:2px 9px;font-size:12px;cursor:pointer;margin-top:6px}.pub:hover{background:#1B3FD1;color:#fff}
.dev{font-size:11.5px;color:#E0B35A;background:#2A2010;border-radius:20px;padding:1px 8px;margin-left:6px}
.pj{display:flex;align-items:center;gap:8px;background:#12275A66;border:1px solid #2A4170;border-radius:12px;padding:8px 10px;font-size:13.5px;animation:pop2 .3s both}
.pj b{flex:1;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;font-weight:600}
.pj button{background:#12203F;border:0;border-radius:8px;padding:3px 9px;font-size:12px;cursor:pointer;color:var(--lb);white-space:nowrap}.pj button:hover{background:#1B3FD1;color:#fff}
.te>button.ed:hover{background:#12203F;color:var(--lb)}
.shake{animation:shk .4s}@keyframes shk{25%{transform:translateX(-6px)}75%{transform:translateX(6px)}}
.lh{display:flex;align-items:center;gap:8px;padding:18px 22px 8px;font-size:13.5px;color:var(--mut)}
.lh b{background:#12203F;color:var(--lb);border-radius:20px;padding:1px 9px;font-weight:500}
#tl{padding:0 18px 20px;display:flex;flex-direction:column;gap:8px}
.te{display:flex;gap:10px;align-items:flex-start;background:#0C1424;border:1px solid var(--line);border-radius:14px;padding:12px 14px;animation:pop2 .3s both;transition:border-color .2s}
.te:hover{border-color:#2A4170}
.tb2{flex:1;min-width:0}.tks{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:6px}.tt{font-size:14px;line-height:1.6;overflow-wrap:anywhere}
.te>button{background:none;border:0;color:var(--mut);cursor:pointer;border-radius:8px;width:28px;height:28px;transition:all .2s}
.te>button:hover{background:#2A1219;color:#FF8A9A}
.empty2{text-align:center;color:var(--mut);padding:20px 0 6px;line-height:1.7;font-size:14px}.empty2 div{font-size:30px;margin-bottom:4px;animation:glow 4s ease-in-out infinite}
.own{text-align:center;font-size:12.5px;color:var(--mut);line-height:1.6;padding:12px 8px 4px;border-top:1px solid var(--line)}
.own b{font-weight:600;color:var(--lb)}
.own span{display:block}
.tag{margin-top:12px;padding:6px 16px;border:1px solid var(--line);border-radius:20px;font-size:13px;color:var(--lb);background:#0E162699;animation:up .8s .9s both}
@media(min-width:761px){aside{animation:sl .7s cubic-bezier(.2,.8,.2,1) both}}
@keyframes sl{from{opacity:0;transform:translateX(-28px)}}
@media(max-width:760px){
 aside{position:fixed;inset:0 auto 0 0;width:min(300px,86vw);transform:translateX(-100%);will-change:transform}
 aside.open{transform:none}
 #ov{position:fixed;inset:0;background:#000A;z-index:4;display:none}
 aside.open+#ov{display:block}
 .brand #sx{display:grid}
 .bar{display:flex}#wel h1{font-size:22px}.imgs img{max-width:150px}
}
.i{width:20px;height:20px;fill:none;stroke:currentColor;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round;flex:none}
.i.s{width:16px;height:16px}
#vc{flex:none;width:38px;height:38px;border-radius:50%;background:var(--blue);border:0;cursor:pointer;display:grid;place-items:center;color:#fff;transition:transform .15s}
#vc:active{transform:scale(.9)}
form.ready #vc{display:none}
form:not(.ready) #go{display:none}
body.nosr #vc{display:none}
body.nosr form:not(.ready) #go{display:grid}
.acts button.on{color:var(--lb)}
#vo{position:fixed;inset:0;z-index:50;background:#05070D;display:none;flex-direction:column;align-items:center;justify-content:space-between;padding:calc(env(safe-area-inset-top,0px) + 16px) 20px calc(env(safe-area-inset-bottom,0px) + 28px)}
#vo.on{display:flex}
#vo .vt{width:100%;display:flex;justify-content:space-between;align-items:center}
#vl{background:var(--panel);border:1px solid var(--line);color:var(--txt);border-radius:999px;padding:9px 18px;cursor:pointer;font-size:14px}
#vm{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:22px;width:100%;min-height:0}
#vb{width:150px;height:150px;border-radius:50%;border:0;cursor:pointer;background:radial-gradient(circle at 35% 30%,#9CC0FF,#2F6BFF 55%,#1B3FD1);flex:none}
#vo.l #vb{animation:orb 1.8s ease-in-out infinite}
#vo.s #vb{animation:orb .9s ease-in-out infinite}
#vo.t #vb{animation:blink 1s ease-in-out infinite}
@keyframes orb{50%{transform:scale(1.12)}}
#vs{font-size:18px;color:var(--txt);text-align:center}
#vtx{max-width:560px;max-height:30vh;overflow:auto;text-align:center;color:var(--mut);line-height:1.6;font-size:16px;padding:0 8px;overflow-wrap:anywhere}
#ve{width:64px;height:64px;border-radius:50%;background:var(--panel);border:1px solid var(--line);color:var(--txt);cursor:pointer;display:grid;place-items:center}
#ve svg{width:26px;height:26px}
@media(hover:none){.it button{opacity:.7}}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
#tools{display:flex;gap:8px;overflow-x:auto;padding:2px 4px 10px;scrollbar-width:none}
#tools::-webkit-scrollbar{display:none}
.tl{flex:none;display:flex;align-items:center;gap:7px;background:#0C1424;border:1px solid var(--line);color:var(--mut);border-radius:999px;padding:7px 14px;font-size:13.5px;cursor:pointer;transition:transform .2s,background .2s,color .2s,border-color .2s,box-shadow .2s}
.tl small{font-size:12px;opacity:.8}
.tl:hover{color:var(--txt);border-color:var(--blue);transform:translateY(-1px)}
.tl.on{background:var(--blue);border-color:var(--blue);color:#fff;box-shadow:0 4px 18px #2F6BFF55}
#mic{flex:none;width:38px;height:38px;border-radius:50%;background:none;border:0;cursor:pointer;display:grid;place-items:center;color:var(--mut);transition:transform .3s,color .2s,background .2s}
#mic:hover{color:var(--lb);background:#12203F;transform:scale(1.1)}
#mic.on{color:#fff;background:#E5384F;animation:pulse 1.1s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 #E5384F88}100%{box-shadow:0 0 0 14px #E5384F00}}
#sug{display:flex;flex-direction:column;gap:2px;max-width:420px;margin:22px auto 6px;width:100%;pointer-events:auto}
#sug button{text-align:left;background:none;border:0;color:var(--txt);border-radius:14px;padding:12px 12px;cursor:pointer;display:flex;flex-direction:row;align-items:center;gap:12px;line-height:1.35;animation:up .6s both;transition:transform .2s,border-color .2s,box-shadow .2s}
#sug button:nth-child(2){animation-delay:.08s}#sug button:nth-child(3){animation-delay:.16s}#sug button:nth-child(4){animation-delay:.24s}
#sug button:hover{background:var(--panel);transform:translateX(3px)}
#sug b{font-weight:500;font-size:16px}#sug small{margin-left:2px}#sug small{color:var(--mut);font-size:12.5px}
.th.fc{width:auto;height:auto;max-width:210px;display:flex;align-items:center;background:#12203F;border:1px solid var(--line);border-radius:14px;padding:12px 16px;font-size:13.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fchip{background:#12203F;border:1px solid var(--line);border-radius:12px;padding:8px 12px;font-size:13.5px;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.toast{position:fixed;left:50%;bottom:130px;transform:translateX(-50%);background:#12203F;border:1px solid var(--line);color:var(--txt);padding:10px 16px;border-radius:14px;z-index:40;max-width:90vw;text-align:center;font-size:14px;animation:up .3s both}
</style>
</head>
<body>
<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#8FB8FF"/><stop offset=".5" stop-color="#2F6BFF"/><stop offset="1" stop-color="#1B3FD1"/></linearGradient></defs></svg>
<aside id="side">
 <div class="brand"><svg viewBox="0 0 32 32"><rect width="32" height="32" rx="8" fill="#05070D" stroke="#1A2744"/><path d="M8 24V8l16 16V12" fill="none" stroke="url(#g)" stroke-width="3.4" stroke-linecap="round" stroke-linejoin="round"/><circle cx="24" cy="7" r="2.2" fill="#9CC0FF"/></svg>Nexan Ai<button type="button" id="sx" aria-label="Close menu" title="Close · বন্ধ করুন"><svg class="i" viewBox="0 0 24 24"><path d="M6 6l12 12M18 6L6 18"/></svg></button></div>
 <button class="sb pri" id="new"><span class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg></span><span class="lb"><b>New chat</b><small>নতুন চ্যাট</small></span></button>
 <button class="sb" id="pb"><span class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/></svg></span><span class="lb"><b>Projects</b><small>প্রজেক্ট</small></span></button>
 <div id="pj" class="pj" style="display:none"></div>
 <div class="sec" id="sec">Recent · সাম্প্রতিক</div>
 <div id="list"></div>
 <button class="sb" id="tb" style="display:none"><span class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M12 3l1.8 5.2L19 10l-5.2 1.8L12 17l-1.8-5.2L5 10l5.2-1.8z"/><path d="M19 16l.7 2 2 .7-2 .7-.7 2-.7-2-2-.7 2-.7z"/></svg></span><span class="lb"><b>Teach Nexan</b><small>Nexan-কে শেখান</small></span></button>
 <div class="own">By <b>Ruhul</b></div>
</aside>
<div id="ov"></div>
<main>
 <div class="bar"><button class="rb" id="menu" aria-label="Menu"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M4 8h16M4 16h10"/></svg></button><b id="bt">Nexan Ai</b><button class="rb" id="nb" aria-label="New chat" title="New chat · নতুন চ্যাট"><svg width="21" height="21" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 4H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-6"/><path d="M18.4 2.6a2 2 0 0 1 2.8 2.8L12 14.6 8 16l1.4-4z"/></svg></button></div>
 <div id="msgs"></div>
 <div id="wel">
  <svg class="big" viewBox="0 0 32 32"><path class="dr" pathLength="1" d="M8 24V8l16 16V12" fill="none" stroke="url(#g)" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/><circle class="sp" cx="24" cy="7" r="2.2" fill="#9CC0FF" style="transform-origin:24px 7px"/></svg>
  <h1>How can I help you today?<small>আজ কীভাবে সাহায্য করতে পারি?</small></h1>
  <p>Chat, search the web, read files, create images<br>চ্যাট করুন, ওয়েব সার্চ করুন, ফাইল পড়ান, ছবি বানান</p>
  <div id="sug">
   <button type="button" data-k="img"><svg class="i" viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="16" rx="3"/><circle cx="9" cy="10" r="1.6"/><path d="M4 18l5-5 4 4 3-3 4 4"/></svg><b>Create an image</b><small>ছবি বানান</small></button>
   <button type="button" data-k="web"><svg class="i" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c3 3.2 3 14.8 0 18M12 3c-3 3.2-3 14.8 0 18"/></svg><b>Search the web</b><small>ইন্টারনেটে খুঁজুন</small></button>
   <button type="button" data-k="file"><svg class="i" viewBox="0 0 24 24"><path d="M7 3h7l5 5v13H7z"/><path d="M14 3v5h5M10 13h6M10 17h6"/></svg><b>Read a PDF or file</b><small>ফাইল পড়ান</small></button>
   <button type="button" data-k="code"><svg class="i" viewBox="0 0 24 24"><path d="M8 8l-4 4 4 4M16 8l4 4-4 4M13.5 6l-3 12"/></svg><b>Solve with code</b><small>হিসাব করান</small></button>
  </div>
  <div class="tag">By Ruhul</div>
 </div>
 <div class="foot">
  <div id="strip"></div>
  <div id="tools">
   <button type="button" id="ws" class="tl" aria-pressed="false"><svg class="i s" viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c3 3.2 3 14.8 0 18M12 3c-3 3.2-3 14.8 0 18"/></svg>Search<small>সার্চ</small></button>
   <button type="button" id="gi" class="tl" aria-pressed="false"><svg class="i s" viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="16" rx="3"/><circle cx="9" cy="10" r="1.6"/><path d="M4 18l5-5 4 4 3-3 4 4"/></svg>Image<small>ছবি</small></button>
   <button type="button" id="cx" class="tl" aria-pressed="false"><svg class="i s" viewBox="0 0 24 24"><path d="M8 8l-4 4 4 4M16 8l4 4-4 4M13.5 6l-3 12"/></svg>Code<small>কোড</small></button>
   <button type="button" id="ar" class="tl" aria-pressed="false"><svg class="i s" viewBox="0 0 24 24"><path d="M4 9v6h4l5 4V5L8 9z"/><path d="M16.5 8.5a5 5 0 0 1 0 7M19 6a8.5 8.5 0 0 1 0 12"/></svg>Auto-read<small>অটো-রিড</small></button>
  </div>
  <form id="f">
   <button type="button" id="at" title="Upload image · ছবি দিন" aria-label="Upload image"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14M5 12h14"/></svg></button>
   <button type="button" id="mic" title="Record voice message · ভয়েস মেসেজ রেকর্ড" aria-label="Voice message"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="3" width="6" height="12" rx="3"/><path d="M5 11a7 7 0 0 0 14 0M12 18v3"/></svg></button>
   <textarea id="in" rows="1"></textarea>
   <button type="button" id="vc" title="Voice conversation · ভয়েস কথোপকথন" aria-label="Voice conversation"><svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M4 10v4M8 7v10M12 4v16M16 8v8M20 11v2"/></svg></button>
   <button id="go" aria-label="Send"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M12 19V5M5 12l7-7 7 7"/></svg></button>
  </form>
 </div>
</main>
<input type="file" id="fi" accept="image/*,application/pdf,audio/*,video/*,.txt,.md,.csv,.json,.html,.js,.py,.css,.xml,.log" multiple hidden>
<div id="vo" role="dialog" aria-label="Voice conversation">
 <div class="vt"><button type="button" id="vl">বাংলা</button><span></span></div>
 <div id="vm"><button type="button" id="vb" aria-label="Tap to interrupt"></button><div id="vs"></div><div id="vtx"></div></div>
 <button type="button" id="ve" aria-label="End voice · শেষ করুন" title="End · শেষ"><svg class="i" viewBox="0 0 24 24"><path d="M6 6l12 12M18 6L6 18"/></svg></button>
</div>
<div id="drop">Drop files here<br>ফাইল এখানে ছাড়ুন</div>
<dialog id="teach">
 <div class="dh"><span class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor"><path d="M12 3l1.8 5.2L19 10l-5.2 1.8L12 17l-1.8-5.2L5 10l5.2-1.8z"/><path d="M19 16l.7 2 2 .7-2 .7-.7 2-.7-2-2-.7 2-.7z"/></svg></span><div><h2>Teach Nexan</h2><div class="sub">Nexan-কে শেখান</div></div><button class="x" id="tc" aria-label="Close">✕</button></div>
 <p class="hint">Add your own facts. When anyone asks about your keywords, Nexan answers with this info.<br>নিজের তথ্য যোগ করুন। যে-কেউ আপনার কীওয়ার্ড নিয়ে প্রশ্ন করলে Nexan এই তথ্য দিয়ে উত্তর দেবে।</p>
 <div class="card">
  <div id="pwrow" class="pw"><input class="fld" id="pw" type="password" placeholder="Admin password · অ্যাডমিন পাসওয়ার্ড" autocomplete="current-password"><button class="btn" id="pwb" type="button">Unlock · খুলুন</button></div>
  <div id="tform">
  <label>1 · Keywords <small>কীওয়ার্ড</small></label>
  <div class="chipbox" id="cb"><span id="chips"></span><input id="tk" placeholder="Type &amp; press Enter · লিখে Enter চাপুন"></div>
  <div class="eg">e.g. fee, price, course, ফি, দাম, কোর্স</div>
  <label>2 · Info <small>তথ্য</small></label>
  <textarea class="fld" id="tt" rows="3" placeholder="Our course fee is 5000 Taka. · আমাদের কোর্সের ফি ৫০০০ টাকা।"></textarea>
  <button class="btn" id="ts">Save · সেভ করুন</button>
  <button class="cp" id="lock" type="button" style="align-self:center">Lock · লক করুন</button>
  </div>
  <div class="eg" id="tmsg" role="status"></div>
 </div>
 <div class="lh">Taught so far · যা শেখানো হয়েছে <b id="cnt">0</b></div>
 <div id="tl"></div>
</dialog>
<dialog id="proj">
 <div class="dh"><span class="ic"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/></svg></span><div><h2>Projects</h2><div class="sub">প্রজেক্ট</div></div><button class="x" id="pc" aria-label="Close">✕</button></div>
 <p class="hint">Create a project with a topic. Every chat inside it stays on that topic.<br>একটি টপিক দিয়ে প্রজেক্ট বানান। এর ভেতরের সব চ্যাট ওই টপিক নিয়েই কথা বলবে। (শুধু এই ব্রাউজারে সেভ হয়)</p>
 <div class="card">
  <label>1 · Project name <small>প্রজেক্টের নাম</small></label>
  <input class="fld" id="pn" maxlength="60" placeholder="e.g. Spoken English · স্পোকেন ইংলিশ" autocomplete="off">
  <label>2 · Topic &amp; instructions <small>টপিক ও নির্দেশনা</small></label>
  <textarea class="fld" id="pt" rows="4" placeholder="This project is about learning English. Reply simply, give examples. · এই প্রজেক্ট ইংরেজি শেখা নিয়ে। সহজ ভাষায় উদাহরণ দিয়ে উত্তর দাও।"></textarea>
  <button class="btn" id="ps">Create project · প্রজেক্ট বানান</button>
  <button class="cp" id="pcx" type="button" style="display:none;align-self:center">Cancel edit · বাতিল</button>
 </div>
 <div class="lh">Your projects · আপনার প্রজেক্ট <b id="pcnt">0</b></div>
 <div id="pl" style="padding:0 18px 20px;display:flex;flex-direction:column;gap:8px"></div>
</dialog>
<script>
const $=s=>document.querySelector(s);
let db=null,chats=[],cur=null,KB=[],busy=false,pend=[],projs=[],curProj=null,editP=null;
const ls={get:k=>{try{return localStorage.getItem(k)||''}catch(e){return ''}},set:(k,v)=>{try{v?localStorage.setItem(k,v):localStorage.removeItem(k)}catch(e){}}};
let PW=ls.get('nx_pw'),LOC=false;
// fetch with the optional site access code (asked once, when the owner turned ACCESS_CODE on)
async function nfetch(u,o={}){
 const go=()=>fetch(u,{...o,headers:{...(o.headers||{}),'x-access-code':ls.get('nx_code')}});
 let r=await go();
 if(r.status===401){const d=await r.clone().json().catch(()=>({}));
  if(d.needCode){const c=prompt('Enter access code · অ্যাক্সেস কোড দিন');if(c){ls.set('nx_code',c.trim());r=await go()}}}
 return r}
const T=(en,bn)=>en+' · '+bn;

// ---- Database: IndexedDB (stores: chats, kb) ----
const idb=()=>new Promise((ok,no)=>{const r=indexedDB.open('bdai',2);
 r.onupgradeneeded=()=>{['chats','kb','projects'].forEach(n=>{if(!r.result.objectStoreNames.contains(n))r.result.createObjectStore(n,{keyPath:'id'})})};
 r.onsuccess=()=>ok(r.result);r.onerror=()=>no(r.error)});
const rq=r=>new Promise((ok,no)=>{r.onsuccess=()=>ok(r.result);r.onerror=()=>no(r.error)});
const st=(n,w)=>db.transaction(n,w?'readwrite':'readonly').objectStore(n);
async function init(){
 try{db=await idb();chats=await rq(st('chats').getAll());projs=await rq(st('projects').getAll());KB=(await rq(st('kb').getAll())).filter(k=>k.u)}catch(e){db=null}
 let base=[],gl=[];
 [base,gl]=await Promise.all([
  fetch('kb.json').then(r=>r.json()).catch(()=>[]),
  fetch('/api/kb',{cache:'no-store'}).then(r=>r.ok?r.json():{}).then(j=>j.entries||[]).catch(()=>[])]);
 KB=base.concat(gl,KB);
 chats.sort((a,b)=>b.t-a.t);side();fresh();holder();
 if(PW||location.hash==='#admin')$('#tb').style.display='';
}

// ---- Knowledge retrieval (keyword match, handles Bangla suffixes) ----
const tk=s=>(s.toLowerCase().match(/[\\p{L}\\p{M}\\p{N}]+/gu)||[]);
function ctx(q){
 const qt=tk(q);
 return KB.map(k=>({k,s:tk(k.keys.join(' ')).filter(w=>qt.some(t=>w.length<3?t===w:t.startsWith(w))).length}))
  .filter(x=>x.s>0).sort((a,b)=>b.s-a.s).slice(0,4).map(x=>x.k.text);
}

// ---- Safe mini-markdown ----
const esc=s=>s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const md=s=>esc(s).replace(/\`\`\`(?:\\w*\\n)?([\\s\\S]*?)\`\`\`/g,'<pre><code>$1</code></pre>')
 .replace(/\`([^\`\\n]+)\`/g,'<code>$1</code>').replace(/\\*\\*([^*\\n]+)\\*\\*/g,'<b>$1</b>').replace(/^#{1,3} (.+)$/gm,'<b>$1</b>');

// ---- Chats ----
const uid=()=>Date.now().toString(36)+Math.random().toString(36).slice(2,7);
function fresh(){stopSpeak();cur={id:uid(),title:'New chat',t:Date.now(),msgs:[],pid:curProj};$('#msgs').innerHTML='';$('#wel').style.display='';side();$('#side').classList.remove('open')}
function load(id){cur=chats.find(c=>c.id===id);$('#msgs').innerHTML='';
 $('#wel').style.display=cur.msgs.length?'none':'';cur.msgs.forEach(m=>{if(m.gen){const b=add('assistant','',1);stopThink();showGen(b,m.gen,m.content)}else add(m.role,m.content,0,m.imgs,m.files)});side();$('#side').classList.remove('open')}
function save(){if(!chats.includes(cur))chats.push(cur);chats.sort((a,b)=>b.t-a.t);if(db)st('chats',1).put(cur)}
function del(id){chats=chats.filter(c=>c.id!==id);if(db)st('chats',1).delete(id);cur&&cur.id===id?fresh():side()}
function side(){const l=$('#list');l.innerHTML='';projUI();const vis=chats.filter(c=>(c.pid||null)===curProj);
 if(!vis.length){l.innerHTML='<div class="empty">No chats yet<br>কোনো চ্যাট নেই</div>';return}
 vis.forEach(c=>{const d=document.createElement('div');d.className='it'+(cur&&c.id===cur.id?' on':'');
  const s=document.createElement('span');s.textContent=c.title;const x=document.createElement('button');x.textContent='✕';x.title=T('Delete','মুছুন');
  x.onclick=e=>{e.stopPropagation();del(c.id)};d.onclick=()=>load(c.id);d.append(s,x);l.append(d)})}

// ---- Messages ----
const LOGO='<svg viewBox="0 0 32 32"><path d="M8 24V8l16 16V12" fill="none" stroke="url(#g)" stroke-width="3.6" stroke-linecap="round" stroke-linejoin="round"/><circle cx="24" cy="7" r="2.4" fill="#9CC0FF"/></svg>';
const IC={
 spk:'<svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 9v6h4l5 4V5L8 9z"/><path d="M16.5 8.5a5 5 0 0 1 0 7M19 6a8.5 8.5 0 0 1 0 12"/></svg>',
 stop:'<svg width="19" height="19" viewBox="0 0 24 24" fill="currentColor"><rect x="6.5" y="6.5" width="11" height="11" rx="2.5"/></svg>',
 copy:'<svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="11" height="11" rx="2.5"/><path d="M5 15V6a2 2 0 0 1 2-2h8"/></svg>',
 dl:'<svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 4v11M7 11l5 5 5-5M5 20h14"/></svg>',
 ok:'<svg width="19" height="19" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12.5l4.5 4.5L19 7.5"/></svg>'};
function actBtn(kind,title,fn){const b=document.createElement('button');b.type='button';b.innerHTML=IC[kind];b.title=title;b.setAttribute('aria-label',title);
 b.onclick=async()=>{try{await fn();b.innerHTML=IC.ok;b.classList.add('ok');setTimeout(()=>{b.innerHTML=IC[kind];b.classList.remove('ok')},1500)}catch(e){toast(T('Could not do that on this device.','এই ডিভাইসে হলো না।'))}};return b}
async function copyText(t){
 try{await navigator.clipboard.writeText(t)}
 catch(e){const a=document.createElement('textarea');a.value=t;a.style.cssText='position:fixed;opacity:0';document.body.append(a);a.select();const ok=document.execCommand('copy');a.remove();if(!ok)throw e}}
function saveBlob(bl,name){const u=URL.createObjectURL(bl),a=document.createElement('a');a.href=u;a.download=name;document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(u),4000)}
async function getBlob(src){const r=await fetch(src);if(!r.ok)throw new Error('img');return await r.blob()}
async function asPng(bl){if(bl.type==='image/png')return bl;const bm=await createImageBitmap(bl),c=document.createElement('canvas');c.width=bm.width;c.height=bm.height;c.getContext('2d').drawImage(bm,0,0);
 return await new Promise((ok,no)=>c.toBlob(x=>x?ok(x):no(new Error('png')),'image/png'))}
function actions(col,o){const old=col.querySelector('.acts');if(old)old.remove();
 const w=document.createElement('div');w.className='acts';
 if(o.img){
  w.append(actBtn('copy',T('Copy image','ছবি কপি'),async()=>{const bl=await asPng(await getBlob(o.img));await navigator.clipboard.write([new ClipboardItem({'image/png':bl})])}));
  w.append(actBtn('dl',T('Download image','ছবি ডাউনলোড'),async()=>{try{saveBlob(await getBlob(o.img),'nexan-image.png')}catch(e){window.open(o.img,'_blank','noopener')}}));
 }
 if(o.text){
  w.append(actBtn('copy',T('Copy text','লেখা কপি'),()=>copyText(o.text)));
  if(SYN){const sb=document.createElement('button');sb.type='button';sb.className='spk';sb.innerHTML=IC.spk;sb.title=T('Read aloud','পড়ে শোনাও');sb.setAttribute('aria-label','Read aloud');
   sb.onclick=()=>{if(spkBtn===sb)stopSpeak();else speakText(o.text,null,sb)};w.append(sb)}
  w.append(actBtn('dl',T('Download text','লেখা ডাউনলোড'),()=>saveBlob(new Blob([o.text],{type:'text/plain;charset=utf-8'}),'nexan-reply.txt')));
 }
 col.append(w)}
function copyBtn(col,text){actions(col,{text})}
function add(role,text,dots,imgs,files){
 const u=role==='user',m=document.createElement('div');m.className='m '+(u?'u':'a');
 if(!u){const a=document.createElement('div');a.className='av'+(dots?' think':'');a.innerHTML=LOGO;m.append(a)}
 const col=document.createElement('div');col.className='col';
 if(imgs&&imgs.length){const w=document.createElement('div');w.className='imgs';imgs.forEach(src=>{const i=document.createElement('img');i.src=src;w.append(i)});col.append(w)}
 if(files&&files.length){const w=document.createElement('div');w.className='imgs';files.forEach(f=>{const c=document.createElement('span');c.className='fchip';c.textContent=fico(f.mime)+' '+f.name;w.append(c)});col.append(w)}
 const b=document.createElement('div');b.className='b';
 if(dots)b.innerHTML='<span class="dots"><span></span><span></span><span></span><span></span><span></span></span>';
 else if(u)b.textContent=text;else b.innerHTML=md(text);
 if(!(u&&!text))col.append(b);if(!u&&!dots&&text)copyBtn(col,text);m.append(col);$('#msgs').append(m);$('#msgs').scrollTop=1e9;return b}
const stopThink=()=>document.querySelectorAll('.av.think').forEach(a=>a.classList.remove('think'));
const type=(b,t)=>new Promise(ok=>{stopThink();b.textContent='';let i=0;const step=Math.max(2,Math.ceil(t.length/150));
 const id=setInterval(()=>{i+=step;b.textContent=t.slice(0,i);$('#msgs').scrollTop=1e9;
  if(i>=t.length){clearInterval(id);b.innerHTML=md(t);copyBtn(b.parentNode,t);ok()}},16)});

// ---- Images: pick, paste, drag-drop (resized to keep uploads small) ----
async function shrink(f){
 const bm=await createImageBitmap(f),k=Math.min(1,1280/Math.max(bm.width,bm.height));
 const c=document.createElement('canvas');c.width=Math.round(bm.width*k);c.height=Math.round(bm.height*k);
 const x=c.getContext('2d');x.fillStyle='#fff';x.fillRect(0,0,c.width,c.height);x.drawImage(bm,0,0,c.width,c.height);
 return{url:c.toDataURL('image/jpeg',.82)}}
const FMAX=2.3e6,TMAX=3.2e6; // one file up to ~2.3 MB, all attachments together ~3.2 MB after base64 (keeps requests small and fast)
const sz=x=>x.file?x.data.length:x.url.length,tot=()=>pend.reduce((a,x)=>a+sz(x),0);
const toast=m=>{const t=document.createElement('div');t.className='toast';t.textContent=m;document.body.append(t);setTimeout(()=>t.remove(),3800)};
const b64=f=>new Promise((ok,no)=>{const r=new FileReader();r.onload=()=>ok(String(r.result).split(',')[1]);r.onerror=no;r.readAsDataURL(f)});
const TXT=/\\.(txt|md|csv|json|html|js|py|css|xml|log)$/i;
const fico=m=>m.startsWith('audio/')?'Audio':m.startsWith('video/')?'Video':m==='application/pdf'?'PDF':'Text';
async function addFiles(fs){
 for(const f of fs){if(pend.length>=4){toast(T('Maximum 4 files at a time.','একসাথে সর্বোচ্চ ৪টি ফাইল।'));break}
  try{
   if(f.type.startsWith('image/')){const im=await shrink(f);if(tot()+im.url.length>TMAX){toast(T('Attachments are too large together (max ~2 MB).','সব ফাইল মিলিয়ে অনেক বড় (সর্বোচ্চ ~২ MB)।'));continue}pend.push(im);continue}
   const ok=f.type==='application/pdf'||/^(audio|video)\\//.test(f.type)||f.type.startsWith('text/')||TXT.test(f.name);
   if(!ok){toast(T('This file type is not supported.','এই ধরনের ফাইল চলবে না।'));continue}
   if(f.size>FMAX){toast(T('File is too big (max 2 MB). Please use a smaller file.','ফাইল অনেক বড় (সর্বোচ্চ ২ MB)। ছোট ফাইল দিন।'));continue}
   const mime=f.type==='application/pdf'||/^(audio|video)\\//.test(f.type)?f.type:'text/plain';
   const dd=await b64(f);if(tot()+dd.length>TMAX){toast(T('Attachments are too large together (max ~2 MB).','সব ফাইল মিলিয়ে অনেক বড় (সর্বোচ্চ ~২ MB)।'));continue}pend.push({file:1,name:f.name||'file',mime,data:dd})
  }catch(e){}}
 strip();upd()}
function strip(){const s=$('#strip');s.innerHTML='';pend.forEach((im,i)=>{const d=document.createElement('div');d.className=im.file?'th fc':'th';
 let g;if(im.file){g=document.createElement('span');g.textContent=fico(im.mime)+' '+im.name}else{g=document.createElement('img');g.src=im.url}
 const x=document.createElement('button');x.type='button';x.textContent='✕';x.title='Remove · সরান';x.setAttribute('aria-label','Remove');
 x.onclick=()=>{pend.splice(i,1);strip();upd()};d.append(g,x);s.append(d)})}
// ---- Voice message: record, convert to WAV (16 kHz), send ----
async function wav16(blob){
 const AC=window.AudioContext||window.webkitAudioContext,ac=new AC(),buf=await ac.decodeAudioData(await blob.arrayBuffer());if(ac.close)ac.close();
 const oc=new OfflineAudioContext(1,Math.max(1,Math.ceil(buf.duration*16000)),16000),src=oc.createBufferSource();src.buffer=buf;src.connect(oc.destination);src.start();
 const d=(await oc.startRendering()).getChannelData(0),v=new DataView(new ArrayBuffer(44+d.length*2)),w=(o,t)=>{for(let k=0;k<t.length;k++)v.setUint8(o+k,t.charCodeAt(k))};
 w(0,'RIFF');v.setUint32(4,36+d.length*2,true);w(8,'WAVEfmt ');v.setUint32(16,16,true);v.setUint16(20,1,true);v.setUint16(22,1,true);v.setUint32(24,16000,true);v.setUint32(28,32000,true);v.setUint16(32,2,true);v.setUint16(34,16,true);w(36,'data');v.setUint32(40,d.length*2,true);
 for(let k=0;k<d.length;k++){const x=Math.max(-1,Math.min(1,d[k]));v.setInt16(44+k*2,x<0?x*32768:x*32767,true)}
 return new Blob([v],{type:'audio/wav'})}
let rec=null;
$('#mic').onclick=async()=>{
 if(rec){rec.stop();return}
 if(busy)return;
 if(!await micGate())return;
 try{
  const st=await navigator.mediaDevices.getUserMedia({audio:true}),chunks=[],mr=new MediaRecorder(st);rec=mr;
  mr.ondataavailable=e=>chunks.push(e.data);
  mr.onstop=async()=>{st.getTracks().forEach(t=>t.stop());rec=null;$('#mic').classList.remove('on');
   try{const w=await wav16(new Blob(chunks,{type:mr.mimeType}));
    if(w.size>FMAX){toast(T('Recording is too long. Keep it under 60 seconds.','রেকর্ডিং অনেক লম্বা। ৬০ সেকেন্ডের মধ্যে রাখুন।'));return}
    pend.push({file:1,name:'voice.wav',mime:'audio/wav',data:await b64(w)});strip();upd();send()
   }catch(e){toast(T('Could not read the recording.','রেকর্ডিং পড়া যায়নি।'))}};
  mr.start();$('#mic').classList.add('on');toast(T('Recording… tap the mic again to send.','রেকর্ড হচ্ছে… পাঠাতে আবার মাইকে চাপুন।'));
  setTimeout(()=>{if(rec===mr)mr.stop()},70000)
 }catch(e){toast(T('Please allow the microphone.','মাইক্রোফোনের অনুমতি দিন।'))}};
$('#at').onclick=()=>$('#fi').click();
$('#fi').onchange=e=>{addFiles([...e.target.files]);e.target.value=''};
addEventListener('paste',e=>{const fs=[...(e.clipboardData?e.clipboardData.files:[])];if(fs.length){e.preventDefault();addFiles(fs)}});
addEventListener('dragover',e=>{e.preventDefault();$('#drop').classList.add('on')});
addEventListener('dragleave',e=>{if(!e.relatedTarget)$('#drop').classList.remove('on')});
addEventListener('drop',e=>{e.preventDefault();$('#drop').classList.remove('on');addFiles([...e.dataTransfer.files])});

// ---- Send ----
// ---- Image generation ----
let genMode=false;
const GEN_RE=[/^\\s*\\/(image|img|draw)\\b/i,/^\\s*(draw|imagine|paint)\\s+\\S/i,
 /\\b(generate|create|make|draw|paint|design)\\b.{0,25}\\b(image|picture|photo|logo|wallpaper|illustration|poster|drawing)\\b/i,
 /(ছবি|ইমেজ|লোগো|পোস্টার|চিত্র).{0,20}(আঁক|এঁক|আকো|আকুন|আকতে|বানাও|বানিয়ে|বানাতে|বানাই|তৈরি|তৈরী|জেনারেট)/];
const isGen=t=>!/\\b(how|why|what is|explain)\\b|কীভাবে|কিভাবে|কেন/i.test(t)&&GEN_RE.some(r=>r.test(t));
const gprompt=t=>t.replace(/^\\s*\\/(image|img|draw)\\s*/i,'').trim()||t;
function setGen(v){genMode=v;$('#gi').classList.toggle('on',v);$('#gi').setAttribute('aria-pressed',v)}
$('#gi').onclick=()=>{setGen(!genMode);$('#in').focus()};
let wsMode=false,cxMode=false;
function setWs(v){wsMode=v;$('#ws').classList.toggle('on',v);$('#ws').setAttribute('aria-pressed',v)}
function setCx(v){cxMode=v;$('#cx').classList.toggle('on',v);$('#cx').setAttribute('aria-pressed',v)}
$('#ws').onclick=()=>{setWs(!wsMode);$('#in').focus()};
$('#cx').onclick=()=>{setCx(!cxMode);$('#in').focus()};
const SUG={
 img:()=>{setGen(true);$('#in').value='A beautiful flower garden at sunrise';},
 web:()=>{setWs(true);$('#in').value="What are today's top news headlines?";},
 code:()=>{setCx(true);$('#in').value='Calculate 15% of 2480 and the square root of 1764, and show the steps';},
 file:()=>$('#fi').click()};
document.querySelectorAll('#sug button').forEach(b=>b.onclick=()=>{SUG[b.dataset.k]();grow();upd();$('#in').focus()});
const pollinate=p=>new Promise((ok,no)=>{
 const u='https://image.pollinations.ai/prompt/'+encodeURIComponent(p)+'?width=1024&height=1024&nologo=true&model=flux&seed='+Math.floor(Math.random()*1e6);
 const i=new Image(),t=setTimeout(()=>no(new Error('timeout')),40000);
 i.onload=()=>{clearTimeout(t);ok(u)};i.onerror=()=>{clearTimeout(t);no(new Error('load'))};i.src=u});
async function makeImage(prompt,images){
 let d={},st=0;
 for(let i=0;i<2;i++){
  try{const r=await nfetch('/api/image',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({prompt,images:images||[]})});
   st=r.status;
   if(r.ok&&(r.headers.get('content-type')||'').startsWith('image/')){const bl=await r.blob();return await new Promise((ok,no)=>{const fr=new FileReader();fr.onload=()=>ok(fr.result);fr.onerror=no;fr.readAsDataURL(bl)})}
   d=await r.json().catch(()=>({}));
   if(r.status===401||(r.status===500&&d.error&&/API_KEY/.test(d.error)))throw new Error(d.error);
   if(r.ok||![429,500,502,503,504].includes(r.status))break}catch(e){if(e&&/Access code|API_KEY/.test(e.message))throw e}
  await new Promise(z=>setTimeout(z,800))}
 if(d.image)return d.image;
 if(images&&images.length)throw new Error(T('Image editing is not available on your current Gemini key.','আপনার বর্তমান Gemini কী-তে ছবি এডিট চালু নেই।'));
 const wait=45000-(Date.now()-(+ls.get('nexa_lastfree')||0));
 if(wait>0)throw new Error(T('Free image limit: please wait '+Math.ceil(wait/1000)+' seconds and try again.','ফ্রি ছবির সীমা: '+Math.ceil(wait/1000)+' সেকেন্ড অপেক্ষা করে আবার চেষ্টা করুন।'));
 ls.set('nexa_lastfree',String(Date.now()));
 const p=d.fallback?d.prompt:prompt;
 for(let i=0;i<2;i++){try{return await pollinate(p)}catch(e){}}
 throw new Error(T('Could not create the image. Please try again.','ছবি বানানো যায়নি। আবার চেষ্টা করুন।'))}
function showGen(b,src,prompt){
 b.innerHTML='';const im=new Image();im.className='gimg';im.alt=prompt;im.src=src;b.append(im);
 actions(b.parentNode,{img:src})}

// ---- Read the streamed reply: words appear while Nexan is still writing ----
async function readStream(r,b){
 const rd=r.body.getReader(),dec=new TextDecoder(),m=$('#msgs');let acc='',last=0,q=0,stall=0;
 const paint=()=>{q=0;last=Date.now();b.innerHTML=md(acc);if(m.scrollHeight-m.scrollTop-m.clientHeight<200)m.scrollTop=1e9};
 const arm=()=>{clearTimeout(stall);stall=setTimeout(()=>rd.cancel().catch(()=>{}),30000)};
 arm();
 try{for(;;){const{done,value}=await rd.read();if(done)break;arm();acc+=dec.decode(value,{stream:true});
  if(acc.trim()){stopThink();if(!q)q=setTimeout(()=>requestAnimationFrame(paint),Math.max(0,50-(Date.now()-last)))}}}catch(e){if(!acc.trim())throw e}
 clearTimeout(stall);clearTimeout(q);q=0;
 acc+=dec.decode();
 if(!acc.trim())throw new Error('Empty reply, please try again · খালি উত্তর, আবার চেষ্টা করুন');
 stopThink();b.innerHTML=md(acc);copyBtn(b.parentNode,acc);m.scrollTop=1e9;return acc}
// no answer headers within 50 s -> stop waiting (the stream itself is not cut)
async function chatFetch(o){const ac=new AbortController(),tm=setTimeout(()=>ac.abort(),50000);
 try{return await nfetch('/api/chat',{...o,signal:ac.signal})}finally{clearTimeout(tm)}}

// ---- Send ----
async function send(){
 let txt=$('#in').value.trim();const im=pend.slice(),auto=!txt;
 if((auto&&!im.length)||busy)return;
 const ip=im.filter(x=>!x.file),fp=im.filter(x=>x.file);
 const edit=genMode&&!auto&&ip.length>0&&!fp.length;
 const gen=!auto&&(edit||(!im.length&&(genMode||isGen(txt))));
 if(auto){const m=fp.length?fp[0].mime:'';
  txt=!fp.length?'Please look at this image and help me. / এই ছবিটা দেখে সাহায্য করো।'
   :m.startsWith('audio/')?'Listen to this audio. If it is a voice message, reply to it; otherwise transcribe and summarize it. / অডিওটা শুনে উত্তর দাও।'
   :m.startsWith('video/')?'Watch this video, describe it and summarize it. / ভিডিওটা দেখে সারাংশ বলো।'
   :'Read this file and give a clear summary. / ফাইলটা পড়ে সারাংশ দাও।'}
 busy=true;$('#go').disabled=true;$('#in').value='';pend=[];strip();grow();upd();$('#wel').style.display='none';setGen(false);
 const fl=fp.map(x=>({name:x.name,mime:x.mime,data:x.data}));
 cur.msgs.push({role:'user',content:txt,imgs:ip.map(x=>x.url),files:fl});
 if(cur.msgs.length===1)cur.title=auto?(fp.length?fico(fp[0].mime)+' '+fp[0].name.slice(0,30):'Image · ছবি'):txt.slice(0,40);
 add('user',auto?'':txt,0,ip.map(x=>x.url),fl);const b=add('assistant','',1);let done=null;stopSpeak();
 try{
  if(gen){
   const pr=gprompt(txt);
   b.innerHTML='<div class="gsk">Creating your image…<br>ছবি তৈরি হচ্ছে…</div>';
   const src=await makeImage(pr,edit?ip.map(x=>({mime:'image/jpeg',data:x.url.split(',')[1]})):null);stopThink();showGen(b,src,pr);
   cur.msgs.push({role:'assistant',content:pr,gen:src});done='';
  }else{
   const li=cur.msgs.map(m=>(m.imgs&&m.imgs.length)||(m.files&&m.files.length)?1:0).lastIndexOf(1);
   const payload=cur.msgs.map((m,i)=>({role:m.role,content:m.content,images:i===li?[...(m.imgs||[]).map(u=>({mime:'image/jpeg',data:u.split(',')[1]})),...(m.files||[]).map(f=>({mime:f.mime,data:f.data}))]:[]}));
   let r,d={};
   for(let i=0;i<3;i++){
    r=await chatFetch({method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({messages:payload,search:wsMode,code:cxMode,voice:!!VC,context:ctx(txt),project:(pr=>pr?{name:pr.name,text:pr.text}:null)(projs.find(p=>p.id===cur.pid))})});
    if(r.ok)break;
    d=await r.json().catch(()=>({}));
    if(![429,500,502,503,504].includes(r.status))break;
    await new Promise(z=>setTimeout(z,1200));
   }
   if(!r.ok)throw new Error(d.error||'Server busy, please retry · সার্ভার ব্যস্ত, আবার পাঠান ('+r.status+')');
   const reply=await readStream(r,b);cur.msgs.push({role:'assistant',content:reply});done=reply;
  }
 }catch(e){stopThink();
  const net=e instanceof TypeError||/Failed to fetch|NetworkError|Load failed/i.test(e.message||'');
  b.textContent=(e&&e.name==='AbortError')?T('Reply took too long. Please try again.','উত্তর আসতে দেরি হচ্ছে। আবার চেষ্টা করুন।'):(net?(/^https?:/.test(location.protocol)?T('Network problem. Check your internet and try again.','নেটওয়ার্ক সমস্যা। ইন্টারনেট দেখে আবার চেষ্টা করুন।'):T('Open your site link (https://…pages.dev), not the downloaded file.','ডাউনলোড করা ফাইল নয়, আপনার সাইটের লিংক (https://…pages.dev) খুলুন।')):e.message);b.classList.add('err')}
 cur.t=Date.now();save();side();busy=false;$('#go').disabled=false;if(!VC)$('#in').focus();
 if(done&&arMode&&!VC){const sb=b.parentNode.querySelector('.spk');speakText(done,null,sb)}
 if(VC)vcNext(done);
}
function grow(){const t=$('#in');t.style.height='auto';t.style.height=Math.min(t.scrollHeight,160)+'px'}
function upd(){$('#f').classList.toggle('ready',!!($('#in').value.trim()||pend.length))}
$('#f').onsubmit=e=>{e.preventDefault();send()};
$('#in').oninput=()=>{grow();upd()};
$('#in').onkeydown=e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();send()}};
$('#new').onclick=fresh;$('#nb').onclick=fresh;
$('#menu').onclick=()=>$('#side').classList.toggle('open');
const closeSide=()=>$('#side').classList.remove('open');
$('#sx').onclick=closeSide;$('#ov').onclick=closeSide;
addEventListener('keydown',e=>{if(e.key==='Escape'){if(VC)vEnd();else closeSide()}});
{let x0=null;const sd=$('#side');
 sd.addEventListener('touchstart',e=>{x0=e.touches[0].clientX},{passive:true});
 sd.addEventListener('touchend',e=>{if(x0!==null&&e.changedTouches[0].clientX-x0<-60)closeSide();x0=null},{passive:true})}

function holder(){$('#in').placeholder='Ask anything'}

// ---- Voice: read aloud + hands-free conversation (free browser voices) ----
const SYN=window.speechSynthesis||null,SR=window.SpeechRecognition||window.webkitSpeechRecognition||null;
if(!SR)document.body.classList.add('nosr');
if(!SYN)$('#ar').style.display='none';
let VOICES=[];const loadV=()=>{try{VOICES=SYN.getVoices()||[]}catch(e){}};
if(SYN){loadV();SYN.onvoiceschanged=loadV}
const isBn=t=>{const b=(t.match(/[\\u0980-\\u09FF]/g)||[]).length,l=(t.match(/\\p{L}/gu)||[]).length;return l>0&&b/l>.3};
function pickVoice(lang){const L=lang.toLowerCase(),p=L.slice(0,2);
 return VOICES.find(v=>v.lang.replace('_','-').toLowerCase()===L)||VOICES.find(v=>v.lang.toLowerCase().startsWith(p))||null}
function speakable(t){return t.replace(/\\n\\nSources:[\\s\\S]*$/,'').replace(/\`\`\`[\\s\\S]*?\`\`\`/g,' ').replace(/\`([^\`]*)\`/g,'$1').replace(/https?:\\/\\/\\S+/g,' ')
 .replace(/^#{1,6}\\s*/gm,'').replace(/^\\s*[-*•]\\s+/gm,'').replace(/[*_#>|~]/g,'').replace(/\\s+/g,' ').trim()}
function chunksOf(t){const parts=t.match(/[^।.!?\\n]+[।.!?]*\\s*/g)||[t],out=[];let c='';
 for(const p of parts){if((c+p).length>170&&c){out.push(c);c=p}else c+=p}if(c.trim())out.push(c);return out}
let spk=0,spkBtn=null;
function spkUI(btn){if(spkBtn){spkBtn.innerHTML=IC.spk;spkBtn.classList.remove('on')}spkBtn=btn||null;if(spkBtn){spkBtn.innerHTML=IC.stop;spkBtn.classList.add('on')}}
function stopSpeak(){spk++;try{SYN&&SYN.cancel()}catch(e){}spkUI(null)}
function speakText(text,done,btn){
 if(!SYN){done&&done();return}
 stopSpeak();const id=spk;spkUI(btn);
 const ch=chunksOf(speakable(text));let i=0;
 const fin=()=>{if(id!==spk)return;spkUI(null);done&&done()};
 const next=()=>{if(id!==spk)return;if(i>=ch.length)return fin();
  const t=ch[i++];if(!/[\\p{L}\\p{N}]/u.test(t))return next();
  const u=new SpeechSynthesisUtterance(t),bn=isBn(t),lang=bn?'bn-BD':'en-US',v=pickVoice(lang)||(bn?pickVoice('bn-IN'):null);
  if(v){u.voice=v;u.lang=v.lang}else u.lang=lang;
  if(bn&&!v&&!ls.get('nx_bnhint')){ls.set('nx_bnhint','1');toast(T('No Bangla voice found. Install it: Phone Settings > Text-to-speech > Google > Bangla.','বাংলা ভয়েস পাওয়া যায়নি। ফোনের Settings > Text-to-speech > Google > বাংলা ডাউনলোড করুন।'))}
  u.rate=1.02;u.onend=next;u.onerror=()=>{if(id===spk)next()};
  try{SYN.speak(u)}catch(e){fin()}};
 next()}
// auto-read chip
let arMode=ls.get('nx_ar')==='1';
function setAr(v){arMode=v;ls.set('nx_ar',v?'1':'');$('#ar').classList.toggle('on',v);$('#ar').setAttribute('aria-pressed',v)}
setAr(arMode);
$('#ar').onclick=()=>{setAr(!arMode);if(!arMode)stopSpeak();else toast(T('Replies will be read aloud.','উত্তর পড়ে শোনানো হবে।'))};

// Microphone gate: asks the browser for "Allow" (only possible on an https page) and explains clearly when it cannot
async function micGate(){
 if(!/^https?:$/.test(location.protocol)||!window.isSecureContext||!navigator.mediaDevices||!navigator.mediaDevices.getUserMedia){
  toast(T('The microphone works only on your https site link, not on a downloaded file. Open https://…pages.dev','মাইক্রোফোন শুধু https সাইট লিংকে চলে, ডাউনলোড করা ফাইলে নয়। https://…pages.dev লিংকে খুলুন।'));return false}
 try{const q=await navigator.permissions.query({name:'microphone'});
  if(q.state==='denied'){toast(T('Microphone is blocked. Tap the icon left of the address bar > Permissions > Microphone > Allow.','মাইক্রোফোন ব্লক করা আছে। ঠিকানার বাঁ পাশের আইকন > Permissions > Microphone > Allow করুন।'));return false}}catch(e){}
 try{const st=await navigator.mediaDevices.getUserMedia({audio:true});st.getTracks().forEach(t=>t.stop());return true}
 catch(e){toast(T('Microphone was not allowed. Tap the mic again and choose Allow.','মাইক্রোফোন Allow করা হয়নি। আবার চাপুন এবং Allow বেছে নিন।'));return false}}

// ---- Hands-free voice conversation: listen -> send -> speak -> listen ----
let VC=null;const vo=$('#vo');
const VS={l:T('Listening…','শুনছি…'),t:T('Thinking…','ভাবছে…'),s:T('Speaking… tap to interrupt','বলছে… থামাতে চাপুন')};
function vState(st){vo.className='on '+st;$('#vs').textContent=VS[st]}
async function vStart(){
 if(VC||!SR)return;
 if(busy){toast(T('Please wait for the reply.','উত্তর শেষ হওয়া পর্যন্ত অপেক্ষা করুন।'));return}
 if(!await micGate())return;
 VC={lang:ls.get('nx_vl')||'bn-BD',tries:0,err:0,rec:null,wl:null};
 $('#vl').textContent=VC.lang==='bn-BD'?'বাংলা':'English';$('#vtx').textContent='';
 $('#wel').style.display='none';
 try{if(navigator.wakeLock)VC.wl=await navigator.wakeLock.request('screen')}catch(e){}
 vListen()}
function vEnd(){const v=VC;VC=null;if(!v)return;
 try{if(v.rec){v.rec.onend=null;v.rec.onerror=null;v.rec.abort()}}catch(e){}
 try{v.wl&&v.wl.release()}catch(e){}
 stopSpeak();vo.className='';if(cur&&!cur.msgs.length)$('#wel').style.display=''}
function vListen(){
 if(!VC)return;const v=VC;let fin='';
 const rec=new SR();v.rec=rec;rec.lang=v.lang;rec.interimResults=true;rec.continuous=false;rec.maxAlternatives=1;
 vState('l');$('#vtx').textContent='';
 rec.onresult=e=>{let it='';for(let i=e.resultIndex;i<e.results.length;i++){const r=e.results[i];if(r.isFinal)fin+=r[0].transcript;else it+=r[0].transcript}
  $('#vtx').textContent=(fin+it).trim()};
 rec.onerror=e=>{if(VC!==v)return;
  if(e.error==='not-allowed'||e.error==='service-not-allowed'){toast(T('Please allow the microphone.','মাইক্রোফোনের অনুমতি দিন।'));vEnd()}
  else if(e.error==='network'){toast(T('Voice needs internet.','ভয়েসের জন্য ইন্টারনেট লাগবে।'));vEnd()}
  else if(e.error==='language-not-supported'){toast(T('This language is not supported for voice on your device.','এই ভাষার ভয়েস আপনার ডিভাইসে চলে না।'));vEnd()}};
 rec.onend=()=>{if(VC!==v)return;const t=fin.trim();
  if(t){v.tries=0;vState('t');$('#in').value=t;send();return}
  if(++v.tries>=4){toast(T('No voice heard. Voice mode ended.','কোনো কথা শোনা যায়নি। ভয়েস মোড শেষ।'));vEnd();return}
  setTimeout(vListen,250)};
 try{rec.start()}catch(e){setTimeout(vListen,400)}}
function vcNext(reply){
 if(!VC)return;
 if(reply===null){if(++VC.err>=2){toast(T('Voice mode stopped because of an error.','ত্রুটির কারণে ভয়েস মোড বন্ধ হয়েছে।'));vEnd();return}vListen();return}
 VC.err=0;
 if(reply){vState('s');speakText(reply,()=>{if(VC)vListen()})}else vListen()}
$('#vc').onclick=vStart;$('#ve').onclick=vEnd;
$('#vb').onclick=()=>{if(VC&&vo.classList.contains('s')){stopSpeak();vListen()}};
$('#vl').onclick=()=>{if(!VC)return;VC.lang=VC.lang==='bn-BD'?'en-US':'bn-BD';ls.set('nx_vl',VC.lang);$('#vl').textContent=VC.lang==='bn-BD'?'বাংলা':'English';
 if(vo.classList.contains('l')){try{VC.rec.onend=null;VC.rec.abort()}catch(e){}vListen()}};

// ---- Teach Nexan (your own knowledge, saved in this browser) ----
let tkeys=[];
function chipsR(){const c=$('#chips');c.innerHTML='';tkeys.forEach((k,i)=>{const s=document.createElement('span');s.className='chip';s.textContent=k;
 const x=document.createElement('button');x.type='button';x.textContent='✕';x.onclick=()=>{tkeys.splice(i,1);chipsR()};s.append(x);c.append(s)})}
function addKey(){$('#tk').value.split(/[,،]/).map(x=>x.trim()).filter(Boolean).forEach(k=>{if(!tkeys.includes(k))tkeys.push(k)});$('#tk').value='';chipsR()}
$('#tk').onkeydown=e=>{if(e.key==='Enter'||e.key===','||e.key==='،'){e.preventDefault();addKey()}
 else if(e.key==='Backspace'&&!$('#tk').value&&tkeys.length){tkeys.pop();chipsR()}};
$('#tk').onblur=addKey;$('#cb').onclick=()=>$('#tk').focus();
const msg=(t,bad)=>{const m=$('#tmsg');m.textContent=t||'';m.style.color=bad?'#FF8A9A':'#8FB8FF'};
function lockUI(){const on=!!PW||LOC;$('#pwrow').style.display=on?'none':'';$('#tform').style.display=on?'flex':'none';$('#lock').style.display=PW?'':'none'}
async function api(body){const r=await fetch('/api/kb',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({...body,password:PW})});
 return{r,d:await r.json().catch(()=>({}))}}
function wrong(){PW='';ls.set('nx_pw','');lockUI();msg('Wrong password · ভুল পাসওয়ার্ড',1)}
function teachList(){const l=$('#tl'),u=KB.filter(k=>k.u);$('#cnt').textContent=u.length;l.innerHTML='';
 u.slice().reverse().forEach(k=>{const d=document.createElement('div');d.className='te';const b=document.createElement('div');b.className='tb2';
  const ks=document.createElement('div');ks.className='tks';k.keys.forEach(q=>{const c=document.createElement('span');c.className='chip s';c.textContent=q;ks.append(c)});
  if(!k.g){const v=document.createElement('span');v.className='dev';v.textContent='This device only · শুধু এই ডিভাইসে';ks.append(v)}
  const p=document.createElement('div');p.className='tt';p.textContent=k.text;b.append(ks,p);
  if(!k.g&&PW&&!LOC){const pb=document.createElement('button');pb.className='pub';pb.type='button';pb.textContent='Publish for everyone · সবার জন্য দিন';
   pb.onclick=async()=>{pb.disabled=true;try{const{r,d}=await api({action:'add',keys:k.keys,text:k.text});
     if(r.ok){KB=KB.filter(q=>q.id!==k.id);if(db)st('kb',1).delete(k.id);KB.push(d.entry);msg('Published ✓ · প্রকাশ হয়েছে ✓');teachList();return}
     if(r.status===401)return wrong()}catch(e){}
    pb.disabled=false;msg('Could not publish · প্রকাশ করা যায়নি',1)};b.append(pb)}
  const x=document.createElement('button');x.textContent='✕';x.title='Delete · মুছুন';
  x.onclick=async()=>{
   if(k.g){if(!PW){msg('Unlock first · আগে আনলক করুন',1);return}
    try{const{r}=await api({action:'delete',id:k.id});if(r.status===401)return wrong();if(!r.ok)throw 0}catch(e){msg('Could not delete · মোছা যায়নি',1);return}}
   else if(db)st('kb',1).delete(k.id);
   KB=KB.filter(q=>q.id!==k.id);teachList()};
  d.append(b,x);l.append(d)});
 if(!u.length)l.innerHTML='<div class="empty2">Nothing taught yet<br>এখনও কিছু শেখানো হয়নি</div>'}
$('#tb').onclick=()=>{lockUI();teachList();$('#teach').showModal();$('#side').classList.remove('open')};
$('#tc').onclick=()=>$('#teach').close();
$('#teach').onclick=e=>{if(e.target===e.currentTarget)e.currentTarget.close()};
$('#pw').onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();$('#pwb').click()}};
$('#pwb').onclick=async()=>{const v=$('#pw').value.trim();if(!v)return;PW=v;msg('Checking… · চেক হচ্ছে…');
 try{const{r}=await api({action:'check'});
  if(r.ok){ls.set('nx_pw',PW);LOC=false;$('#pw').value='';msg('');lockUI();teachList();return}
  if(r.status===401)return wrong();
  PW='';LOC=true;lockUI();msg('Server storage is not ready. Saving on this device only. · সার্ভার প্রস্তুত নয়, শুধু এই ডিভাইসে সেভ হবে।',1)
 }catch(e){PW='';msg('Network problem, try again · নেটওয়ার্ক সমস্যা, আবার চেষ্টা করুন',1)}};
$('#lock').onclick=()=>{PW='';LOC=false;ls.set('nx_pw','');lockUI();msg('')};
$('#ts').onclick=async()=>{addKey();const t=$('#tt').value.trim();
 if(!tkeys.length||!t){const c=$('#teach .card');c.classList.remove('shake');void c.offsetWidth;c.classList.add('shake');return}
 const keys=tkeys.slice();let e=null,ok='';
 if(!LOC){
  try{const{r,d}=await api({action:'add',keys,text:t});
   if(r.ok){e=d.entry;ok='Saved for everyone ✓ · সবার জন্য সেভ হয়েছে ✓'}
   else if(r.status===401)return wrong();
   else throw 0
  }catch(x){msg('Server not reachable. Saved on this device only. · সার্ভারে সেভ হয়নি, শুধু এই ডিভাইসে সেভ হয়েছে।',1)}}
 if(!e){e={id:'u_'+uid(),u:1,keys,text:t};if(db)st('kb',1).put(e)}
 KB.push(e);tkeys=[];chipsR();$('#tt').value='';teachList();if(ok)msg(ok)};
// ---- Projects (a topic + instructions; chats inside stay on that topic; saved in this browser) ----
const pst=()=>db&&st('projects',1);
function welcome(){const pr=projs.find(p=>p.id===curProj),w=$('#wel h1'),p=$('#wel p');
 if(!pr){w.innerHTML='How can I help you today?<small>আজ কীভাবে সাহায্য করতে পারি?</small>';p.innerHTML='Ask anything or upload a screenshot<br>যেকোনো কিছু জিজ্ঞাসা করুন বা স্ক্রিনশট দিন';return}
 w.textContent=pr.name;const s=document.createElement('small');s.textContent='Ask anything about this project · এই প্রজেক্ট নিয়ে জিজ্ঞাসা করুন';w.append(s);
 p.textContent=pr.text.slice(0,140)+(pr.text.length>140?'…':'')}
function projUI(){const pr=projs.find(p=>p.id===curProj),b=$('#pj');
 $('#sec').textContent=pr?'Project chats · প্রজেক্টের চ্যাট':'Recent · সাম্প্রতিক';
 $('#bt').textContent=pr?'Nexan Ai · '+pr.name:'Nexan Ai';
 b.style.display=pr?'':'none';b.innerHTML='';
 if(pr){const n=document.createElement('b');n.textContent=pr.name;const x=document.createElement('button');x.type='button';x.textContent='✕ Exit · বের হন';x.onclick=()=>{curProj=null;fresh()};b.append(n,x)}
 welcome()}
function resetPF(){editP=null;$('#pn').value='';$('#pt').value='';$('#ps').textContent='Create project · প্রজেক্ট বানান';$('#pcx').style.display='none'}
function openProj(id){curProj=id;$('#proj').close();fresh()}
function delProj(p){if(!confirm('Delete project "'+p.name+'" and its chats?\\nপ্রজেক্ট "'+p.name+'" ও এর সব চ্যাট মুছবেন?'))return;
 chats.filter(c=>c.pid===p.id).forEach(c=>{if(db)st('chats',1).delete(c.id)});chats=chats.filter(c=>c.pid!==p.id);
 projs=projs.filter(q=>q.id!==p.id);if(pst())pst().delete(p.id);
 if(curProj===p.id){curProj=null;fresh()}else side();projList()}
function projList(){const l=$('#pl');l.innerHTML='';$('#pcnt').textContent=projs.length;
 projs.slice().reverse().forEach(p=>{const d=document.createElement('div');d.className='te';d.style.cursor='pointer';
  const b=document.createElement('div');b.className='tb2';
  const t=document.createElement('div');t.className='tt';t.style.fontWeight='600';t.textContent=p.name;
  const n=chats.filter(c=>c.pid===p.id).length,s=document.createElement('div');s.className='tt';s.style.cssText='color:var(--mut);font-size:13px';
  s.textContent=p.text.slice(0,110)+(p.text.length>110?'…':'')+' · '+n+' chats · চ্যাট';b.append(t,s);
  const e=document.createElement('button');e.className='ed';e.type='button';e.textContent='✎';e.title='Edit · সম্পাদনা';
  e.onclick=ev=>{ev.stopPropagation();editP=p.id;$('#pn').value=p.name;$('#pt').value=p.text;$('#ps').textContent='Update · আপডেট করুন';$('#pcx').style.display='';$('#pn').focus()};
  const x=document.createElement('button');x.type='button';x.textContent='✕';x.title='Delete · মুছুন';x.onclick=ev=>{ev.stopPropagation();delProj(p)};
  d.onclick=()=>openProj(p.id);d.append(b,e,x);l.append(d)});
 if(!projs.length)l.innerHTML='<div class="empty2">No projects yet<br>এখনও কোনো প্রজেক্ট নেই</div>'}
$('#pb').onclick=()=>{resetPF();projList();$('#proj').showModal();$('#side').classList.remove('open')};
$('#pc').onclick=()=>$('#proj').close();
$('#proj').onclick=e=>{if(e.target===e.currentTarget)e.currentTarget.close()};
$('#pcx').onclick=resetPF;
$('#ps').onclick=()=>{const n=$('#pn').value.trim().slice(0,60),t=$('#pt').value.trim().slice(0,3000);
 if(!n||!t){const c=$('#proj .card');c.classList.remove('shake');void c.offsetWidth;c.classList.add('shake');return}
 if(editP){const p=projs.find(q=>q.id===editP);p.name=n;p.text=t;if(pst())pst().put(p);resetPF();projList();projUI();return}
 const p={id:'p_'+uid(),name:n,text:t,t:Date.now()};projs.push(p);if(pst())pst().put(p);resetPF();openProj(p.id)};
console.log("%cNexan Ai · By Ruhul","color:#2F6BFF;font-size:14px;font-weight:bold");
init();
</script>
</body>
</html>
`;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const ip = request.headers.get('CF-Connecting-IP') || 'x';
    const p = url.pathname;
    try {
      if (p === '/api/kb') return await kb(request, env, ip);
      if (p === '/api/chat' || p === '/api/image') {
        if (request.method !== 'POST') return J(405, { error: 'Method not allowed' });
        if (env.ACCESS_CODE && !safeEq(request.headers.get('x-access-code') || '', env.ACCESS_CODE))
          return J(401, { error: 'Access code needed · অ্যাক্সেস কোড দিন', needCode: true });
        if (limited(ip, p, p === '/api/chat' ? 40 : 8, 60000)) return J(429, { error: 'Too many requests, wait a minute · এক মিনিট অপেক্ষা করুন' });
        return p === '/api/chat' ? await chat(request, env, ctx) : await image(request, env);
      }
    } catch (err) {
      return J(500, { error: err.message || 'Server error' });
    }
    if (env.ASSETS) return env.ASSETS.fetch(request);
    if (p === '/kb.json') return new Response(KB_JSON, { headers: { 'Content-Type': 'application/json; charset=utf-8' } });
    if (request.method !== 'GET' && request.method !== 'HEAD') return J(405, { error: 'Method not allowed' });
    return new Response(INDEX_HTML, { headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' } });
  },
};
