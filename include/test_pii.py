from include.pii import mask_email, mask_phone
print(mask_email("priya.sharma@example.com"))
print(mask_phone("9876543210"))
print(mask_phone("98765"))
print(mask_email(""))