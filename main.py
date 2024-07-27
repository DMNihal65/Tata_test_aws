# main.py
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import boto3
import os
from botocore.exceptions import ClientError

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Document(Base):
    __tablename__ = "documents"

    id = Column(Integer, primary_key=True, index=True)
    part_number = Column(String, unique=True, index=True)
    file_name = Column(String)
    s3_key = Column(String)


Base.metadata.create_all(bind=engine)

# S3 client setup
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                  aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                  )
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class UploadResponse(BaseModel):
    message: str
    part_number: str


class DocumentResponse(BaseModel):
    file_name: str
    download_url: str


@app.post("/upload/", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...), part_number: str = None, db: Session = Depends(get_db)):
    if not part_number:
        raise HTTPException(status_code=400, detail="Part number is required")

    try:
        s3_key = f"documents/{part_number}/{file.filename}"
        s3.upload_fileobj(file.file, BUCKET_NAME, s3_key)

        # Store metadata in the database
        db_document = Document(part_number=part_number, file_name=file.filename, s3_key=s3_key)
        db.add(db_document)
        db.commit()

        return {"message": "File uploaded successfully", "part_number": part_number}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/document/{part_number}", response_model=DocumentResponse)
async def get_document(part_number: str, db: Session = Depends(get_db)):
    try:
        # Retrieve metadata from the database
        document = db.query(Document).filter(Document.part_number == part_number).first()
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")

        # Generate presigned URL
        url = s3.generate_presigned_url('get_object',
                                        Params={'Bucket': BUCKET_NAME, 'Key': document.s3_key},
                                        ExpiresIn=3600)

        return {"file_name": document.file_name, "download_url": url}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)