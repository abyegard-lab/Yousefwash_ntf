# NFT Free-Mint Auto Buyer V2

بوت شراء تلقائي لـ Free Mint عبر SeaDrop على Ink مع اكتشاف OpenSea Stream.

## أهم تغييرات V2
- القرار النهائي للسعر والمرحلة من blockchain وليس OpenSea.
- `mintPrice` يجب أن يكون 0 فعليًا.
- `eth_call` قبل الإرسال لتقليل معاملات الـrevert.
- لا يوجد fallback إلى `ZERO_ADDRESS` عندما تكون fee recipients مقيدة.
- الكمية الافتراضية 1، والحد الأعلى 5.
- تقدير الغاز من transaction الفعلية.
- timeout للـreceipt = `pending` وليس failure.
- إعادة المحاولة فقط للأخطاء الشبكية/nonce، وليس contract revert بشكل أعمى.
- SQLite لحفظ عمليات الشراء والحالة بعد إعادة التشغيل.
- OpenSea cache قصير جدًا للـmint state.
- heartbeat حقيقي للـWebSocket بدل اعتبار عدم وجود أحداث = انقطاع.

## Environment

```env
BOT_ENABLED=true
OPENSEA_API_KEY=...
INK_RPC_URL=https://rpc-gel.inkonchain.com/
MAX_GAS_FEE_USD=0.01
REQUESTED_QUANTITY=1
MAX_PARALLEL_DISCOVERY=8
POLL_INTERVAL=2
STATE_DB=nft_bot.sqlite3

PRIVATE_KEYS=key1,key2
WALLETS=0x...,0x...
TELEGRAM_BOT_TOKENS=token1,token2
TELEGRAM_CHAT_IDS=chat1,chat2

# اختياري
TWITTER_BEARER_TOKEN=...
```

> لا تضع private keys داخل الكود أو Git.

## تشغيل

```bash
pip install -r requirements.txt
python main.py
```

## سياسة الغاز

`MAX_GAS_FEE_USD=0.01` تعني أن البوت لن يرسل المعاملة إذا تجاوزت أقصى تكلفة غاز محسوبة بهذا الحد. هذا قد يمنع بعض عمليات الـmint عندما يكون الغاز أعلى من الحد.

## ملاحظة مهمة

النسخة V2 تدعم تنفيذ `mintPublic` فقط. مراحل allowlist/team/presale يتم اكتشافها، لكن لا يتم تجاوز الأهلية الخاصة بها.
