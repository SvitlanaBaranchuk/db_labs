CREATE
UNIQUE INDEX index_school ON
School (Name, Place_ID)
WHERE ParentName IS NULL;