function advancedGmailFilter() {
  const CONFIG = {
    spreadsheetId: '1UoDD1V7I5T6thL_K1FvfZUhhFMzYYHjBL2JgWu2NHRE',
    sheetName: 'voice仙台メール返信用リスト',
    timeRange: '1d', // 検索対象期間
    targetAddress: '*****************',
    pageSize: 100, // Gmail検索の1ページ件数（最大100程度が安定）
    maxPages: 20, // 念のための上限（合計最大 ~2000件）
    bodySnippetLength: 500,
    replyCountWarn: { soft: 2, mid: 3, hard: 5 }, // 色分け基準
    tz: Session.getScriptTimeZone() || 'Asia/Tokyo'
  };

  const HEADERS = [
    '受信日時', '送信者', '宛先', 'CC', '件名', '本文（抜粋）',
    'フィルタールール', '優先度', '返信タイプ', '返信回数', 'キーワード', '処理日時', 'MessageID', 'ThreadID'
  ];

  // ルールは今後増やせるよう配列で
  const filterRules = [
    {
      name: '二回目以降の返信（Re:付き）',
      // deliveredto: はエイリアスにも効くケースが多いが、取りこぼし防止のため後段でコード側でも判定強化
      query: `is:unread subject:(Re: OR 返信:) newer_than:${CONFIG.timeRange} deliveredto:${CONFIG.targetAddress}`,
      priority: 'MEDIUM',
      action: 'TRACK_REPLY',
      labelColor: '#FFAA00'
    }
  ];

  const ss = SpreadsheetApp.openById(CONFIG.spreadsheetId);
  const sheet = getOrCreateSheet_(ss, CONFIG.sheetName, HEADERS);
  const state = PropertiesService.getScriptProperties();
  const processedKey = 'processedMessageIds';
  const processedSet = new Set(JSON.parse(state.getProperty(processedKey) || '[]'));

  let rowsToAppend = [];
  let coloredRanges = []; // [{row, color}]

  filterRules.forEach(rule => {
    processFilterRule_({
      rule, CONFIG, HEADERS, sheet,
      processedSet, rowsToAppend, coloredRanges
    });
  });

  // まとめて書き込み
  if (rowsToAppend.length > 0) {
    const startRow = sheet.getLastRow() + 1;
    sheet.getRange(startRow, 1, rowsToAppend.length, HEADERS.length)
      .setValues(rowsToAppend);

    // 色付け（返信回数）
    const replyCountColumn = HEADERS.indexOf('返信回数') + 1;
    coloredRanges.forEach(item => {
      sheet.getRange(item.row, replyCountColumn).setBackground(item.color);
    });

    // 凍結＆太字（初回のみ念のため）
    if (sheet.getFrozenRows() === 0) sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, HEADERS.length).setFontWeight('bold');
  }

  // 処理済みIDを保存（上限超えたら半分に間引き）
  const finalList = Array.from(processedSet);
  const cap = 5000;
  if (finalList.length > cap) {
    PropertiesService.getScriptProperties()
      .setProperty(processedKey, JSON.stringify(finalList.slice(finalList.length - Math.floor(cap / 2))));
  } else {
    PropertiesService.getScriptProperties()
      .setProperty(processedKey, JSON.stringify(finalList));
  }
}

// ---- 内部関数群 ----

function processFilterRule_({ rule, CONFIG, HEADERS, sheet, processedSet, rowsToAppend, coloredRanges }) {
  try {
    let start = 0;
    let page = 0;
    let totalFound = 0;

    while (page < CONFIG.maxPages) {
      const threads = GmailApp.search(rule.query, start, CONFIG.pageSize);
      if (!threads || threads.length === 0) break;

      totalFound += threads.length;

      threads.forEach(thread => {
        const messages = thread.getMessages();
        messages.forEach(message => {
          // 未読のみ（検索クエリで絞っているが念のため）
          if (!message.isUnread()) return;

          // 返信か判定（件名 or ヘッダで）
          if (!isReplyMessage_(message)) return;

          const messageId = message.getId();

          // 重複防止：既に処理済みならスキップ
          if (processedSet.has(messageId)) return;

          const data = extractMessageData_({ message, rule, CONFIG, thread });
          const row = makeRow_(data, HEADERS);
          rowsToAppend.push(row);

          // 色付け情報を保存（書き込み後の行番号を逆算）
          const futureRow = sheet.getLastRow() + rowsToAppend.length; // まだ書いてないので +length
          const color = colorForReplyCount_(data.replyCount, CONFIG);
          if (color) coloredRanges.push({ row: futureRow, color });

          // アクション（ラベリング等）
          executeAction_({ message, thread, rule, replyCount: data.replyCount });

          // 処理済みに登録
          processedSet.add(messageId);

          // 未読を自動既読化（仕様どおり）
          message.markRead();
        });
      });

      // 次ページ
      start += CONFIG.pageSize;
      page++;
    }

    console.log(`${rule.name}: ${totalFound}件のスレッドを走査`);
  } catch (error) {
    console.error(`フィルタールール「${rule.name}」でエラー: ${error && error.message ? error.message : error}`);
  }
}

function isReplyMessage_(message) {
  const subject = message.getSubject() || '';
  const hasRe = /^(Re:|返信:|RE:|re:)/i.test(subject);

  // 件名が変わっても返信であるケース：ヘッダで判定
  const inReplyTo = safeHeader_(message, 'In-Reply-To');
  const references = safeHeader_(message, 'References');
  const hasHeaders = Boolean(inReplyTo || references);

  return hasRe || hasHeaders;
}

function safeHeader_(message, name) {
  try {
    return message.getHeader(name);
  } catch (_) {
    return '';
  }
}

function extractMessageData_({ message, rule, CONFIG, thread }) {
  const date = message.getDate();
  const from = message.getFrom() || '';
  const to = message.getTo() || '';
  const cc = message.getCc() || '';
  const subject = message.getSubject() || '';
  const body = (message.getPlainBody() || '').replace(/\r\n/g, '\n');

  const bodyTrimmed = trimReplyJunk_(body).slice(0, CONFIG.bodySnippetLength);
  const replyCount = analyzeReplyCount_(thread);
  const replyType = determineReplyType_(subject, bodyTrimmed);
  const keywords = extractKeywords_(subject, bodyTrimmed);

  return {
    timestamp: date,
    from,
    to,
    cc,
    subject,
    body: bodyTrimmed,
    filterRule: rule.name,
    priority: rule.priority,
    replyType,
    replyCount,
    keywords: keywords.join(', '),
    processingTime: new Date(),
    messageId: message.getId(),
    threadId: thread.getId()
  };
}

function analyzeReplyCount_(thread) {
  const messages = thread.getMessages();
  let count = 0;
  messages.forEach(m => {
    const subj = m.getSubject() || '';
    const hasRe = /^(Re:|返信:|RE:|re:)/i.test(subj);
    const hasHdr = Boolean(safeHeader_(m, 'In-Reply-To') || safeHeader_(m, 'References'));
    if (hasRe || hasHdr) count++;
  });
  return count;
}

function determineReplyType_(subject, body) {
  const text = `${subject}\n${body}`;
  if (/(ありがとうございます|承知|了解|確認しました|助かります)/.test(text)) return '確認・承諾';
  if (/(質問|疑問|わからない|教えて|詳細お願いします|もう少し)/.test(text)) return '追加質問';
  if (/(急ぎ|至急|緊急|すぐ|本日中|大至急)/.test(text)) return '緊急返信';
  return '通常返信';
}

function extractKeywords_(subject, body) {
  const text = `${subject} ${body}`;
  const dict = [
    '見積', '価格', '料金', '費用',
    '納期', 'スケジュール', '期限', '締切',
    '仕様', '要件', '機能', '原稿', '写真', '入稿',
    '契約', '合意', '条件', '請求', '支払い',
    '問題', 'トラブル', 'エラー', '不具合',
    '緊急', '重要', '至急', '確認', '修正', '差し替え'
  ];
  return dict.filter(k => text.indexOf(k) !== -1);
}

function trimReplyJunk_(body) {
  // 引用開始行以降をざっくり落とす（日本語メールでよくあるパターンに対応）
  const lines = body.split('\n');
  const cutIdx = lines.findIndex(l =>
    /^>/.test(l) ||
    /^On .+wrote:/.test(l) ||
    /^-----Original Message-----/.test(l) ||
    /^From: /.test(l) ||
    /^差出人: /.test(l) ||
    /^--\s*$/.test(l) // シグネチャ区切り
  );
  return (cutIdx >= 0 ? lines.slice(0, cutIdx) : lines).join('\n').trim();
}

function executeAction_({ message, thread, rule, replyCount }) {
  switch (rule.action) {
    case 'TRACK_REPLY': {
      // ベースラベル
      const base = ensureLabel_('返信追跡');
      thread.addLabel(base);

      // 頻度ラベル
      if (replyCount >= 3) {
        const freq = ensureLabel_('頻繁な返信');
        thread.addLabel(freq);
      }
      break;
    }
    default:
      break;
  }
}

function ensureLabel_(name) {
  return GmailApp.getUserLabelByName(name) || GmailApp.createLabel(name);
}

function makeRow_(data, HEADERS) {
  // 日時はそのまま Date で入れる（スプレッドシート側で時刻表示フォーマット推奨）
  return [
    data.timestamp,
    data.from,
    data.to,
    data.cc,
    data.subject,
    data.body,
    data.filterRule,
    data.priority,
    data.replyType,
    data.replyCount,
    data.keywords,
    data.processingTime,
    data.messageId,
    data.threadId
  ];
}

function colorForReplyCount_(count, CONFIG) {
  if (count >= CONFIG.replyCountWarn.hard) return '#FFD700'; // 濃い黄
  if (count >= CONFIG.replyCountWarn.mid) return '#FFFF99'; // 薄い黄
  if (count >= CONFIG.replyCountWarn.soft) return '#FFFFCC'; // ごく薄い黄
  return null;
}

function getOrCreateSheet_(ss, name, HEADERS) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) sheet = ss.insertSheet(name);

  // ヘッダーが未設定ならセット
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);
    sheet.getRange(1, 1, 1, HEADERS.length).setFontWeight('bold');
    sheet.setFrozenRows(1);
  } else {
    // 既存ヘッダー不足時の補完（列ズレ回避）
    const lastCol = sheet.getLastColumn();
    if (lastCol < HEADERS.length) {
      sheet.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);
    }
  }
  return sheet;
}

/** 定期実行の登録（30分ごと） */
function setupTriggers() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'advancedGmailFilter') {
      ScriptApp.deleteTrigger(t);
    }
  });
  ScriptApp.newTrigger('advancedGmailFilter').timeBased().everyMinutes(30).create();
}

/** 手動実行 */
function runReplyFilter() {
  console.log('返信フィルターを手動実行します...');
  advancedGmailFilter();
  console.log('返信フィルター実行完了');
}
