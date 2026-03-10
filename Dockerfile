# استخدم النسخة دي عشان سريعة ومستقرة [cite: 2]
FROM quay.io/astronomer/astro-runtime:12.6.0

# السطر ده بيمنع الـ timeout لو النت بطيء
ENV PIP_DEFAULT_TIMEOUT=100