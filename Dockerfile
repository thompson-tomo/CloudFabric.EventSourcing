FROM mcr.microsoft.com/dotnet/sdk:9.0-bookworm-slim AS build

RUN apt-get update

#---------------------------------------------------------------------
# Tools setup
#---------------------------------------------------------------------
RUN dotnet tool install --global dotnet-ef
RUN dotnet tool install --global coverlet.console
RUN dotnet tool install --global dotnet-reportgenerator-globaltool

# sonarcloud
RUN dotnet tool install --global dotnet-sonarscanner
RUN apt-get update && apt-get install -y default-jdk
#sonarcloud

ENV PATH="/root/.dotnet/tools:${PATH}"

RUN curl -L https://raw.githubusercontent.com/Microsoft/artifacts-credprovider/master/helpers/installcredprovider.sh | sh
#---------------------------------------------------------------------
# /Tools setup
#---------------------------------------------------------------------


#---------------------------------------------------------------------
# Test database setup
#---------------------------------------------------------------------
RUN apt-get install -y postgresql postgresql-client postgresql-contrib

USER postgres

RUN POSTGRES_VERSION=$(ls /etc/postgresql | head -n 1) && \
    echo "Detected PostgreSQL version: $POSTGRES_VERSION" && \
    echo "local   all   all               md5" >> /etc/postgresql/15/main/pg_hba.conf && \
    echo "host    all   all   0.0.0.0/0   md5" >> /etc/postgresql/15/main/pg_hba.conf && \
    echo "listen_addresses='*'" >> /etc/postgresql/15/main/postgresql.conf
    
RUN service postgresql start \
    && psql --command "CREATE ROLE cloudfabric_eventsourcing_test WITH CREATEDB NOSUPERUSER NOCREATEROLE INHERIT NOREPLICATION CONNECTION LIMIT -1 LOGIN PASSWORD 'cloudfabric_eventsourcing_test';" \
    && psql --command "DROP DATABASE IF EXISTS cloudfabric_eventsourcing_test;" \
    && psql --command "CREATE DATABASE cloudfabric_eventsourcing_test WITH OWNER = cloudfabric_eventsourcing_test ENCODING = 'UTF8' CONNECTION LIMIT = -1;" \
    && psql --command "GRANT ALL ON DATABASE cloudfabric_eventsourcing_test TO postgres;"
#---------------------------------------------------------------------
# /Test database setup
#---------------------------------------------------------------------

USER root
WORKDIR /

#---------------------------------------------------------------------
# Test elasticsearch setup
#---------------------------------------------------------------------
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.5.0-amd64.deb
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.5.0-amd64.deb.sha512
RUN shasum -a 512 -c elasticsearch-8.5.0-amd64.deb.sha512
RUN dpkg -i elasticsearch-8.5.0-amd64.deb
# Replace home dir - needed for su
RUN sed -i "s|elasticsearch\(.*\)\/nonexistent\(.*\)|elasticsearch\1/usr/share/elasticsearch\2|g" /etc/passwd
# Replace shell
RUN sed -i "s|elasticsearch\(.*\)\/bin\/false|elasticsearch\1/bin/bash|g" /etc/passwd
RUN sed -i "s|xpack.security.enabled: true|xpack.security.enabled: false|g" /etc/elasticsearch/elasticsearch.yml
RUN sed -i "s|cluster.initial_master_nodes:\(.*\)|# cluster.initial_master_nodes:\1|g" /etc/elasticsearch/elasticsearch.yml
RUN printf '%s\n' 'cluster.routing.allocation.disk.watermark.low: "1gb"' \
    'cluster.routing.allocation.disk.watermark.high: "500mb"' \
    'cluster.routing.allocation.disk.watermark.flood_stage: "500mb"' \
    'cluster.info.update.interval: "30m"' >> /etc/elasticsearch/elasticsearch.yml
#---------------------------------------------------------------------
# /Test elasticsearch setup
#---------------------------------------------------------------------

#---------------------------------------------------------------------
# Test opensearch setup
#---------------------------------------------------------------------
RUN wget https://artifacts.opensearch.org/releases/bundle/opensearch/2.18.0/opensearch-2.18.0-linux-x64.deb
RUN wget https://artifacts.opensearch.org/releases/bundle/opensearch/2.18.0/opensearch-2.18.0-linux-x64.deb.sig
RUN dpkg -i opensearch-2.18.0-linux-x64.deb
# Replace home dir - needed for su
RUN sed -i "s|opensearch\(.*\)\/nonexistent\(.*\)|opensearch\1/usr/share/opensearch\2|g" /etc/passwd
# Replace shell
RUN sed -i "s|opensearch\(.*\)\/bin\/false|opensearch\1/bin/bash|g" /etc/passwd
RUN printf '%s\n' 'plugins.security.disabled: true' \
    'cluster.routing.allocation.disk.watermark.low: "1gb"' \
    'cluster.routing.allocation.disk.watermark.high: "500mb"' \
    'cluster.routing.allocation.disk.watermark.flood_stage: "500mb"' \
    'cluster.info.update.interval: "30m"' >> /etc/opensearch/opensearch.yml
#---------------------------------------------------------------------
# /Test opensearch setup
#---------------------------------------------------------------------

#---------------------------------------------------------------------
# Nuget restore
# !Important: this is a nice hack to avoid package restoration on each docker build step.
# Since we only copy nuget.config and csproj files, docker will not run restore unless nuget.config or csproj files change.
#---------------------------------------------------------------------
#COPY nuget.config /src/nuget.config

COPY CloudFabric.EventSourcing.EventStore/CloudFabric.EventSourcing.EventStore.csproj /src/CloudFabric.EventSourcing.EventStore/CloudFabric.EventSourcing.EventStore.csproj
COPY CloudFabric.EventSourcing.Tests/CloudFabric.EventSourcing.Tests.csproj /src/CloudFabric.EventSourcing.Tests/CloudFabric.EventSourcing.Tests.csproj
COPY CloudFabric.Projections/CloudFabric.Projections.csproj /src/CloudFabric.Projections/CloudFabric.Projections.csproj
COPY CloudFabric.Projections.Attributes/CloudFabric.Projections.Attributes.csproj /src/CloudFabric.Projections.Attributes/CloudFabric.Projections.Attributes.csproj
COPY CloudFabric.Projections.Worker/CloudFabric.Projections.Worker.csproj /src/CloudFabric.Projections.Worker/CloudFabric.Projections.Worker.csproj

COPY Implementations/CloudFabric.EventSourcing.EventStore.InMemory/CloudFabric.EventSourcing.EventStore.InMemory.csproj /src/Implementations/CloudFabric.EventSourcing.EventStore.InMemory/CloudFabric.EventSourcing.EventStore.InMemory.csproj
COPY Implementations/CloudFabric.EventSourcing.Tests.InMemory/CloudFabric.EventSourcing.Tests.InMemory.csproj /src/Implementations/CloudFabric.EventSourcing.Tests.InMemory/CloudFabric.EventSourcing.Tests.InMemory.csproj
COPY Implementations/CloudFabric.Projections.InMemory/CloudFabric.Projections.InMemory.csproj /src/Implementations/CloudFabric.Projections.InMemory/CloudFabric.Projections.InMemory.csproj

COPY Implementations/CloudFabric.EventSourcing.EventStore.CosmosDb/CloudFabric.EventSourcing.EventStore.CosmosDb.csproj /src/Implementations/CloudFabric.EventSourcing.EventStore.CosmosDb/CloudFabric.EventSourcing.EventStore.CosmosDb.csproj
COPY Implementations/CloudFabric.EventSourcing.Tests.CosmosDb/CloudFabric.EventSourcing.Tests.CosmosDb.csproj /src/Implementations/CloudFabric.EventSourcing.Tests.CosmosDb/CloudFabric.EventSourcing.Tests.CosmosDb.csproj
COPY Implementations/CloudFabric.Projections.CosmosDb/CloudFabric.Projections.CosmosDb.csproj /src/Implementations/CloudFabric.Projections.CosmosDb/CloudFabric.Projections.CosmosDb.csproj

COPY Implementations/CloudFabric.EventSourcing.EventStore.Postgresql/CloudFabric.EventSourcing.EventStore.Postgresql.csproj /src/Implementations/CloudFabric.EventSourcing.EventStore.Postgresql/CloudFabric.EventSourcing.EventStore.Postgresql.csproj
COPY Implementations/CloudFabric.EventSourcing.Tests.Postgresql/CloudFabric.EventSourcing.Tests.Postgresql.csproj /src/Implementations/CloudFabric.EventSourcing.Tests.Postgresql/CloudFabric.EventSourcing.Tests.Postgresql.csproj
COPY Implementations/CloudFabric.Projections.Postgresql/CloudFabric.Projections.Postgresql.csproj /src/Implementations/CloudFabric.Projections.Postgresql/CloudFabric.Projections.Postgresql.csproj

COPY Implementations/CloudFabric.EventSourcing.Tests.ElasticSearch/CloudFabric.EventSourcing.Tests.ElasticSearch.csproj /src/Implementations/CloudFabric.EventSourcing.Tests.ElasticSearch/CloudFabric.EventSourcing.Tests.ElasticSearch.csproj
COPY Implementations/CloudFabric.Projections.ElasticSearch/CloudFabric.Projections.ElasticSearch.csproj /src/Implementations/CloudFabric.Projections.ElasticSearch/CloudFabric.Projections.ElasticSearch.csproj

COPY Implementations/CloudFabric.EventSourcing.Tests.OpenSearch/CloudFabric.EventSourcing.Tests.OpenSearch.csproj /src/Implementations/CloudFabric.EventSourcing.Tests.OpenSearch/CloudFabric.EventSourcing.Tests.OpenSearch.csproj
COPY Implementations/CloudFabric.Projections.OpenSearch/CloudFabric.Projections.OpenSearch.csproj /src/Implementations/CloudFabric.Projections.OpenSearch/CloudFabric.Projections.OpenSearch.csproj

COPY CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.csproj /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.csproj
COPY CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.CosmosDb/CloudFabric.EventSourcing.AspNet.CosmosDb.csproj /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.CosmosDb/CloudFabric.EventSourcing.AspNet.CosmosDb.csproj
COPY CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.ElasticSearch/CloudFabric.EventSourcing.AspNet.ElasticSearch.csproj /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.ElasticSearch/CloudFabric.EventSourcing.AspNet.ElasticSearch.csproj
COPY CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.InMemory/CloudFabric.EventSourcing.AspNet.InMemory.csproj /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.InMemory/CloudFabric.EventSourcing.AspNet.InMemory.csproj
COPY CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.Postgresql/CloudFabric.EventSourcing.AspNet.Postgresql.csproj /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.Postgresql/CloudFabric.EventSourcing.AspNet.Postgresql.csproj
COPY CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.OpenSearch/CloudFabric.EventSourcing.AspNet.OpenSearch.csproj /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.OpenSearch/CloudFabric.EventSourcing.AspNet.OpenSearch.csproj

COPY CloudFabric.EventSourcing.Domain/CloudFabric.EventSourcing.Domain.csproj /src/CloudFabric.EventSourcing.Domain/CloudFabric.EventSourcing.Domain.csproj

#RUN dotnet restore /src/CloudFabric.EventSourcing.Domain/CloudFabric.EventSourcing.Domain.csproj
#---------------------------------------------------------------------
# /Nuget restore
#---------------------------------------------------------------------

#---------------------------------------------------------------------
# Build artifacts
#
# you can copy sql migration scripts and coverage report files with following script:
# containerId=$(docker create $(docker images --format='{{.ID}}' | head -1)
# docker cp $containerId:/artifacts/* ./artifacts
# docker rm -v $containerId
#
# !Important all commands must be run in one layer since we will have to copy them later
# and we don't want to run copy commands on 4 different layers
#---------------------------------------------------------------------
COPY /. /src

ARG PULLREQUEST_TARGET_BRANCH
ARG PULLREQUEST_BRANCH
ARG PULLREQUEST_ID
ARG BRANCH_NAME

# Sonar args
ARG SONAR_PROJECT_KEY=Tech-Fabric_CloudFabric.EventSourcing
ARG SONAR_OGRANIZAION_KEY=tech-fabric
ARG SONAR_HOST_URL=https://sonarcloud.io
ARG SONAR_TOKEN
ARG GITHUB_TOKEN
# end sonar args

# Sonar scanner has two different modes - PR and regular with different set of options
RUN if [ -n "$SONAR_TOKEN" ] && [ -n "$PULLREQUEST_TARGET_BRANCH" ] ; then \
        echo "Running sonarscanner in pull request mode: sonar.pullrequest.base=$PULLREQUEST_TARGET_BRANCH, sonar.pullrequest.branch=$PULLREQUEST_BRANCH, sonar.pullrequest.key=$PULLREQUEST_ID" && \
        dotnet sonarscanner begin \
          /k:"$SONAR_PROJECT_KEY" \
          /o:"$SONAR_OGRANIZAION_KEY" \
          /d:sonar.projectBaseDir="/src/" \
          /d:sonar.host.url="$SONAR_HOST_URL" \
          /d:sonar.login="$SONAR_TOKEN" \
          /d:sonar.pullrequest.base="$PULLREQUEST_TARGET_BRANCH" \
          /d:sonar.pullrequest.branch="$PULLREQUEST_BRANCH" \
          /d:sonar.pullrequest.key="$PULLREQUEST_ID" \
          /d:sonar.cs.opencover.reportsPaths=/artifacts/tests/*/coverage.opencover.xml ; \
    elif [ -n "$SONAR_TOKEN" ] ; then \
        echo "Running sonarscanner in branch mode: sonar.branch.name=$BRANCH_NAME" && \
        dotnet sonarscanner begin \
          /k:"$SONAR_PROJECT_KEY" \
          /o:"$SONAR_OGRANIZAION_KEY" \
          /d:sonar.projectBaseDir="/src/" \
          /d:sonar.host.url="$SONAR_HOST_URL" \
          /d:sonar.login="$SONAR_TOKEN" \
          /d:sonar.branch.name="$BRANCH_NAME" \
          /d:sonar.cs.opencover.reportsPaths=/artifacts/tests/*/coverage.opencover.xml ; \
    fi


# Elasticsearch tests require both elastic and postgresql
RUN su elasticsearch -c '/usr/share/elasticsearch/bin/elasticsearch' & service postgresql start && sleep 20 && \
    dotnet test /src/Implementations/CloudFabric.EventSourcing.Tests.ElasticSearch/CloudFabric.EventSourcing.Tests.ElasticSearch.csproj --logger trx --results-directory /artifacts/tests --configuration Release --collect:"XPlat Code Coverage" -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=json,cobertura,lcov,teamcity,opencover

# OpenSearch tests require both opensearch and postgresql
RUN su opensearch -c '/usr/share/opensearch/bin/opensearch -d -p /tmp/opensearch.pid' & service postgresql start && sleep 20 && \
    dotnet test /src/Implementations/CloudFabric.EventSourcing.Tests.OpenSearch/CloudFabric.EventSourcing.Tests.OpenSearch.csproj --logger trx --results-directory /artifacts/tests --configuration Release --collect:"XPlat Code Coverage" -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=json,cobertura,lcov,teamcity,opencover

RUN service postgresql start && \
    dotnet test /src/Implementations/CloudFabric.EventSourcing.Tests.Postgresql/CloudFabric.EventSourcing.Tests.Postgresql.csproj --logger trx --results-directory /artifacts/tests --configuration Release --collect:"XPlat Code Coverage" -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=json,cobertura,lcov,teamcity,opencover

RUN dotnet test /src/Implementations/CloudFabric.EventSourcing.Tests.InMemory/CloudFabric.EventSourcing.Tests.InMemory.csproj --logger trx --results-directory /artifacts/tests --configuration Release --collect:"XPlat Code Coverage" -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=json,cobertura,lcov,teamcity,opencover

ARG COVERAGE_REPORT_GENERATOR_LICENSE
ARG COVERAGE_REPORT_TITLE
ARG COVERAGE_REPORT_TAG
ARG COVERAGE_REPORT_GENERATOR_HISTORY_DIRECTORY

RUN reportgenerator "-reports:/artifacts/tests/*/coverage.cobertura.xml" -targetdir:/artifacts/code-coverage "-reporttypes:HtmlInline_AzurePipelines_Light;SonarQube;TextSummary" "-title:$COVERAGE_REPORT_TITLE" "-tag:$COVERAGE_REPORT_TAG" "-license:$COVERAGE_REPORT_GENERATOR_LICENSE" "-historydir:$COVERAGE_REPORT_GENERATOR_HISTORY_DIRECTORY"

# End Sonar Scanner
RUN if [ -n "$SONAR_TOKEN" ] ; then dotnet sonarscanner end /d:sonar.login="$SONAR_TOKEN" ; fi

ARG PACKAGE_VERSION

RUN sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.EventSourcing.Domain/CloudFabric.EventSourcing.Domain.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.EventSourcing.EventStore/CloudFabric.EventSourcing.EventStore.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.Projections/CloudFabric.Projections.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.Projections.Attributes/CloudFabric.Projections.Attributes.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.Projections.Worker/CloudFabric.Projections.Worker.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/Implementations/CloudFabric.EventSourcing.EventStore.CosmosDb/CloudFabric.EventSourcing.EventStore.CosmosDb.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/Implementations/CloudFabric.EventSourcing.EventStore.InMemory/CloudFabric.EventSourcing.EventStore.InMemory.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/Implementations/CloudFabric.EventSourcing.EventStore.Postgresql/CloudFabric.EventSourcing.EventStore.Postgresql.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/Implementations/CloudFabric.Projections.CosmosDb/CloudFabric.Projections.CosmosDb.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/Implementations/CloudFabric.Projections.InMemory/CloudFabric.Projections.InMemory.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/Implementations/CloudFabric.Projections.Postgresql/CloudFabric.Projections.Postgresql.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/Implementations/CloudFabric.Projections.ElasticSearch/CloudFabric.Projections.ElasticSearch.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/Implementations/CloudFabric.Projections.OpenSearch/CloudFabric.Projections.OpenSearch.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.CosmosDb/CloudFabric.EventSourcing.AspNet.CosmosDb.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.ElasticSearch/CloudFabric.EventSourcing.AspNet.ElasticSearch.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.InMemory/CloudFabric.EventSourcing.AspNet.InMemory.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.Postgresql/CloudFabric.EventSourcing.AspNet.Postgresql.csproj && \
    sed -i "s|<Version>.*</Version>|<Version>$PACKAGE_VERSION</Version>|g" /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.OpenSearch/CloudFabric.EventSourcing.AspNet.OpenSearch.csproj && \
    dotnet pack /src/CloudFabric.EventSourcing.Domain/CloudFabric.EventSourcing.Domain.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.EventSourcing.EventStore/CloudFabric.EventSourcing.EventStore.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.Projections/CloudFabric.Projections.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.Projections.Attributes/CloudFabric.Projections.Attributes.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.Projections.Worker/CloudFabric.Projections.Worker.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/Implementations/CloudFabric.EventSourcing.EventStore.CosmosDb/CloudFabric.EventSourcing.EventStore.CosmosDb.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/Implementations/CloudFabric.EventSourcing.EventStore.InMemory/CloudFabric.EventSourcing.EventStore.InMemory.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/Implementations/CloudFabric.EventSourcing.EventStore.Postgresql/CloudFabric.EventSourcing.EventStore.Postgresql.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/Implementations/CloudFabric.Projections.CosmosDb/CloudFabric.Projections.CosmosDb.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/Implementations/CloudFabric.Projections.InMemory/CloudFabric.Projections.InMemory.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/Implementations/CloudFabric.Projections.Postgresql/CloudFabric.Projections.Postgresql.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/Implementations/CloudFabric.Projections.ElasticSearch/CloudFabric.Projections.ElasticSearch.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.CosmosDb/CloudFabric.EventSourcing.AspNet.CosmosDb.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.ElasticSearch/CloudFabric.EventSourcing.AspNet.ElasticSearch.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.InMemory/CloudFabric.EventSourcing.AspNet.InMemory.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.Postgresql/CloudFabric.EventSourcing.AspNet.Postgresql.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/Implementations/CloudFabric.Projections.OpenSearch/CloudFabric.Projections.OpenSearch.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg && \
    dotnet pack /src/CloudFabric.EventSourcing.AspNet/CloudFabric.EventSourcing.AspNet.OpenSearch/CloudFabric.EventSourcing.AspNet.OpenSearch.csproj -o /artifacts/nugets -p:IncludeSymbols=true -p:SymbolPackageFormat=snupkg

ARG NUGET_API_KEY
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.Domain.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.EventStore.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.Projections.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.Projections.Attributes.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.Projections.Worker.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.EventStore.CosmosDb.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.EventStore.InMemory.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.EventStore.Postgresql.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.Projections.CosmosDb.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.Projections.InMemory.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.Projections.Postgresql.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.Projections.ElasticSearch.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.AspNet.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.AspNet.CosmosDb.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.AspNet.ElasticSearch.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.AspNet.InMemory.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.AspNet.Postgresql.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.Projections.OpenSearch.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
RUN if [ -n "$NUGET_API_KEY" ] ; then dotnet nuget push /artifacts/nugets/CloudFabric.EventSourcing.AspNet.OpenSearch.$PACKAGE_VERSION.nupkg --skip-duplicate -k $NUGET_API_KEY -s https://api.nuget.org/v3/index.json ; fi
#---------------------------------------------------------------------
# /Build artifacts
#---------------------------------------------------------------------