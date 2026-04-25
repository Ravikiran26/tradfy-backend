import os
import resend

resend.api_key = os.getenv("RESEND_API_KEY", "")

FROM = "Traders Diary <support@tradersdiary.in>"


def _send(to: str, subject: str, html: str):
    if not resend.api_key:
        return
    try:
        resend.Emails.send({"from": FROM, "to": [to], "subject": subject, "html": html})
    except Exception:
        pass  # Never block the payment flow on email failure


def send_payment_confirmation(email: str, name: str, expires_at: str):
    first = name.split()[0] if name else "Trader"
    _send(
        to=email,
        subject="You're now Pro on Traders Diary 🎉",
        html=f"""
        <div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:32px 24px;background:#060c18;color:#e2e8f0;border-radius:16px">
          <div style="font-size:28px;font-weight:900;color:#ffffff;margin-bottom:8px">Traders Diary</div>
          <div style="font-size:12px;color:#6366f1;letter-spacing:2px;text-transform:uppercase;margin-bottom:32px">Pro activated</div>

          <p style="font-size:16px;margin:0 0 16px">Hi {first},</p>
          <p style="font-size:16px;margin:0 0 24px">Your Pro subscription is active. You now have <strong style="color:#a5b4fc">unlimited AI analyses</strong> on every trade.</p>

          <div style="background:#0d1528;border:1px solid #1c2e4a;border-radius:12px;padding:20px;margin-bottom:28px">
            <div style="font-size:12px;color:#64748b;margin-bottom:4px">Plan</div>
            <div style="font-size:16px;font-weight:700;color:#fff">Pro — ₹499/mo</div>
            <div style="font-size:12px;color:#64748b;margin-top:12px;margin-bottom:4px">Access until</div>
            <div style="font-size:15px;font-weight:600;color:#a5b4fc">{expires_at}</div>
          </div>

          <p style="font-size:14px;color:#475569;margin:0 0 8px">Your subscription will <strong>not auto-renew</strong>. You will need to manually renew before the expiry date to continue unlimited access.</p>
          <p style="font-size:14px;color:#475569;margin:0 0 32px">If you have any questions, reply to this email or write to <a href="mailto:support@tradersdiary.in" style="color:#6366f1">support@tradersdiary.in</a>.</p>

          <a href="https://tradersdiary.in/dashboard" style="display:inline-block;background:linear-gradient(135deg,#4f46e5,#7c3aed);color:white;font-weight:700;font-size:14px;padding:12px 24px;border-radius:10px;text-decoration:none">
            Go to Dashboard →
          </a>

          <p style="font-size:11px;color:#334155;margin-top:40px">Traders Diary · Not investment advice · Educational tool only</p>
        </div>
        """,
    )


def send_renewal_reminder(email: str, name: str, expires_at: str, days_left: int):
    first = name.split()[0] if name else "Trader"
    urgency = "⚠️ Expiring soon" if days_left > 2 else "🚨 Expiring tomorrow"
    _send(
        to=email,
        subject=f"{urgency} — Your Traders Diary Pro expires in {days_left} day{'s' if days_left != 1 else ''}",
        html=f"""
        <div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:32px 24px;background:#060c18;color:#e2e8f0;border-radius:16px">
          <div style="font-size:28px;font-weight:900;color:#ffffff;margin-bottom:8px">Traders Diary</div>
          <div style="font-size:12px;color:#f59e0b;letter-spacing:2px;text-transform:uppercase;margin-bottom:32px">Pro expiring soon</div>

          <p style="font-size:16px;margin:0 0 16px">Hi {first},</p>
          <p style="font-size:16px;margin:0 0 24px">Your Pro subscription expires in <strong style="color:#fbbf24">{days_left} day{'s' if days_left != 1 else ''}</strong> on <strong>{expires_at}</strong>.</p>
          <p style="font-size:14px;color:#94a3b8;margin:0 0 28px">After expiry, you'll revert to the free plan (10 AI analyses lifetime). Your trades and journal data are never deleted.</p>

          <a href="https://tradersdiary.in/?checkout=pro" style="display:inline-block;background:linear-gradient(135deg,#4f46e5,#7c3aed);color:white;font-weight:700;font-size:14px;padding:12px 24px;border-radius:10px;text-decoration:none">
            Renew Pro — ₹499/mo →
          </a>

          <p style="font-size:11px;color:#334155;margin-top:40px">Traders Diary · Not investment advice · Educational tool only</p>
        </div>
        """,
    )


def send_pro_expired(email: str, name: str):
    first = name.split()[0] if name else "Trader"
    _send(
        to=email,
        subject="Your Traders Diary Pro has expired",
        html=f"""
        <div style="font-family:sans-serif;max-width:520px;margin:0 auto;padding:32px 24px;background:#060c18;color:#e2e8f0;border-radius:16px">
          <div style="font-size:28px;font-weight:900;color:#ffffff;margin-bottom:8px">Traders Diary</div>
          <div style="font-size:12px;color:#64748b;letter-spacing:2px;text-transform:uppercase;margin-bottom:32px">Pro expired</div>

          <p style="font-size:16px;margin:0 0 16px">Hi {first},</p>
          <p style="font-size:16px;margin:0 0 24px">Your Pro subscription has expired. You're back on the free plan — your trades and journal are all still there.</p>

          <a href="https://tradersdiary.in/?checkout=pro" style="display:inline-block;background:linear-gradient(135deg,#4f46e5,#7c3aed);color:white;font-weight:700;font-size:14px;padding:12px 24px;border-radius:10px;text-decoration:none">
            Renew Pro — ₹499/mo →
          </a>

          <p style="font-size:11px;color:#334155;margin-top:40px">Traders Diary · Not investment advice · Educational tool only</p>
        </div>
        """,
    )
