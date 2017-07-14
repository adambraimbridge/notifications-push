FROM alpine:3.5

COPY . .git /notifications-push/

RUN apk --update add git go libc-dev \
  && export GOPATH=/gopath \
  && REPO_PATH="github.com/Financial-Times/notifications-push" \
  && mkdir -p $GOPATH/src/${REPO_PATH} \
  && mv /notifications-push/* $GOPATH/src/${REPO_PATH} \
  && cd $GOPATH/src/${REPO_PATH} \
  && BUILDINFO_PACKAGE="github.com/Financial-Times/service-status-go/buildinfo." \
  && VERSION="version=$(git describe --tag --always 2> /dev/null)" \
  && DATETIME="dateTime=$(date -u +%Y%m%d%H%M%S)" \
  && REPOSITORY="repository=$(git config --get remote.origin.url)" \
  && REVISION="revision=$(git rev-parse HEAD)" \
  && BUILDER="builder=$(go version)" \
  && LDFLAGS="-X '"${BUILDINFO_PACKAGE}$VERSION"' -X '"${BUILDINFO_PACKAGE}$DATETIME"' -X '"${BUILDINFO_PACKAGE}$REPOSITORY"' -X '"${BUILDINFO_PACKAGE}$REVISION"' -X '"${BUILDINFO_PACKAGE}$BUILDER"'" \
  && echo $LDFLAGS \
  && go get -t ./... \
  && go build -ldflags="${LDFLAGS}" \
  && mv notifications-push /notifications-push-app \
  && rm -rf /notifications-push \
  && mv /notifications-push-app /notifications-push \
  && apk del go git libc-dev \
  && rm -rf $GOPATH /var/cache/apk/*

CMD [ "/notifications-push" ]
