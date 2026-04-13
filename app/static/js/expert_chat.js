(() => {
  const els = {
    form: document.getElementById('expert-form'),
    text: document.getElementById('expert-text'),
    send: document.getElementById('expert-send'),
    clear: document.getElementById('expert-clear'),
    messages: document.getElementById('expert-messages'),
  };

  if (!els.form || !els.text || !els.send || !els.clear || !els.messages) return;

  const STORAGE_KEY = 'me_expert_chat_v1';

  function nowIso() {
    try { return new Date().toISOString(); } catch (_) { return null; }
  }

  function readState() {
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      return raw ? JSON.parse(raw) : { history: [], agent_state: null };
    } catch (_) {
      return { history: [], agent_state: null };
    }
  }

  function writeState(state) {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch (_) {}
  }

  function scrollToBottom() {
    try { els.messages.scrollTop = els.messages.scrollHeight; } catch (_) {}
  }

  function addBubble(role, text) {
    const wrap = document.createElement('div');
    wrap.className = `expert-message ${role}`;
    const bubble = document.createElement('div');
    bubble.className = 'bubble';
    bubble.textContent = String(text || '');
    wrap.appendChild(bubble);
    els.messages.appendChild(wrap);
    scrollToBottom();
  }

  function renderFromHistory(history) {
    // Keep the first static assistant message already in HTML, remove everything after it
    while (els.messages.children.length > 1) els.messages.removeChild(els.messages.lastChild);
    (history || []).forEach((m) => {
      if (!m || !m.role) return;
      addBubble(m.role, m.text || '');
    });
  }

  async function postChat(message, agentState) {
    const r = await fetch('/api/expert-chat', {
      method: 'POST',
      credentials: 'include',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message, state: agentState || null }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) {
      const msg = d && (d.error || d.detail) ? (d.error || d.detail) : `Request failed (${r.status})`;
      throw new Error(msg);
    }
    return d;
  }

  function setBusy(busy) {
    els.send.disabled = !!busy;
    els.text.disabled = !!busy;
    if (!busy) els.text.focus();
  }

  const initial = readState();
  if (initial.history && initial.history.length) renderFromHistory(initial.history);

  els.clear.addEventListener('click', () => {
    writeState({ history: [], agent_state: null });
    renderFromHistory([]);
    addBubble('system', 'Chat cleared.');
  });

  els.form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = (els.text.value || '').trim();
    if (!msg) return;
    els.text.value = '';

    const state = readState();
    state.history = state.history || [];
    state.history.push({ role: 'user', text: msg, ts: nowIso() });
    writeState(state);
    addBubble('user', msg);

    setBusy(true);
    addBubble('assistant', 'Thinking…');
    const thinkingNode = els.messages.lastChild;

    try {
      const resp = await postChat(msg, state.agent_state);
      const answer = resp && resp.answer ? String(resp.answer) : 'Sorry — I could not generate an answer.';
      const nextState = resp && typeof resp.state === 'object' ? resp.state : null;

      // Replace "Thinking…" bubble content
      if (thinkingNode && thinkingNode.querySelector) {
        const b = thinkingNode.querySelector('.bubble');
        if (b) b.textContent = answer;
        thinkingNode.className = 'expert-message assistant';
      } else {
        addBubble('assistant', answer);
      }

      state.history.push({ role: 'assistant', text: answer, ts: nowIso() });
      state.agent_state = nextState;
      writeState(state);
    } catch (err) {
      const em = (err && err.message) ? err.message : 'Network error';
      if (thinkingNode && thinkingNode.querySelector) {
        const b = thinkingNode.querySelector('.bubble');
        if (b) b.textContent = `Error: ${em}`;
        thinkingNode.className = 'expert-message system';
      } else {
        addBubble('system', `Error: ${em}`);
      }
    } finally {
      setBusy(false);
    }
  });
})();

