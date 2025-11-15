# File: iac/terraform/main.tf (Example AWS EC2 deployment)

resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
  tags = { Name = "batonics-challenge-vpc" }
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.main.id
}

resource "aws_subnet" "public" {
  vpc_id     = aws_vpc.main.id
  cidr_block = "10.0.1.0/24"
  map_public_ip_on_launch = true
  availability_zone = "us-east-1a" # ⚠️ Update AZ/Region as needed
}

resource "aws_security_group" "api_sg" {
  vpc_id = aws_vpc.main.id
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "app_server" {
  ami           = "ami-0abcdef1234567890" # ⚠️ Replace with valid, Docker-compatible AMI (e.g., Amazon Linux 2)
  instance_type = "t3.small"
  subnet_id     = aws_subnet.public.id
  security_groups = [aws_security_group.api_sg.name]
  key_name      = "my-ssh-key" # ⚠️ Replace with your SSH key
  
  # User data script to install Docker and run the API application
  user_data = <<-EOF
              #!/bin/bash
              sudo yum update -y
              sudo amazon-linux-extras install docker -y
              sudo service docker start
              sudo usermod -a -G docker ec2-user
              # ⚠️ REPLACE with your actual Docker command to run the FastAPI app
              # e.g., docker run -d -p 8000:8000 your-repo/batonics-app:latest
              EOF
  
  tags = {
    Name = "batonics-api-server"
  }
}