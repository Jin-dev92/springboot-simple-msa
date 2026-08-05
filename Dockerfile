# 서비스 4개가 모두 이 파일 하나를 공유한다.
# Spring Boot 실행 가능 jar 는 서비스마다 내용만 다를 뿐 실행 방법이 같기 때문이다.
# 어떤 모듈을 담을지는 compose 가 SERVICE 빌드 인자로 알려 준다.
FROM eclipse-temurin:21-jre

ARG SERVICE
WORKDIR /app

# ./gradlew build 로 미리 만들어 둔 jar 를 그대로 담는다.
# (Docker 안에서 빌드하면 매번 의존성을 새로 받느라 느려지므로 학습용으로는 부적합하다.)
COPY ${SERVICE}/build/libs/*.jar app.jar

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
