package org.microbean.servicebroker.dynamo;

import org.microbean.servicebroker.api.ServiceBroker;
import org.microbean.servicebroker.api.ServiceBrokerException;
import org.microbean.servicebroker.api.command.*;
import org.microbean.servicebroker.api.command.state.Operation;
import org.microbean.servicebroker.api.query.state.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DynamodbServiceBroker extends ServiceBroker {
    private static final Logger logger = LoggerFactory.getLogger(DynamodbServiceBroker.class);
    protected Catalog catalog = null;
    public static final String PROPERTIES_NAME = "datasource-credentialsKey";  //TODO: change the key here
    public static final String DEFAULT_PROPERTIES_NAME = "connection.properties";  //TODO: change the key here

    public DynamodbServiceBroker() {
        super();
    }

    @Override
    public Catalog getCatalog() throws ServiceBrokerException {
        if(catalog == null) {
            this.catalog = populateCatalog();
        }

        return catalog;
    }

    @Override
    public ProvisionBindingCommand.Response execute(ProvisionBindingCommand provisionBindingCommand) throws ServiceBrokerException {
        String bindingId = provisionBindingCommand.getBindingInstanceId();
        String serviceInstanceId = provisionBindingCommand.getServiceInstanceId();
        //String connectionKey = null;  // the key in the connection secret to locate the credentials

        logBind(String.format("called with serviceInstanceId: %s", serviceInstanceId));
        logBind(String.format("called with bindingId: %s", bindingId));

        Map<? extends String, ?> parameters = provisionBindingCommand.getParameters();

        // debug log the parameters
        if(parameters != null && parameters.size()> 0) {

            StringBuffer sb = new StringBuffer();

            for (String key : parameters.keySet()) {
                sb.append(key);
                sb.append(":");
                sb.append(parameters.get(key));
                sb.append("\n");
            }

            logger.info("CreateServiceInstanceBindingRequest contains parameters: " + sb.toString());
        } else {
            logger.info("CreateServiceInstanceBindingRequest contains no parameters");
        }


        /** Need to convert all params to a single string which can be read as a proprties object by the client app */
        String propertiesName = pullParameter(parameters, PROPERTIES_NAME);
        StringBuffer sb = new StringBuffer();
        if(isEmpty(propertiesName)) {
            propertiesName = DEFAULT_PROPERTIES_NAME;
            logBind("No property file name specified via parameter: " + PROPERTIES_NAME + ", using " + propertiesName);
        }

        for(Object key : parameters.keySet()) {
            logBind(key + " = " + parameters.get(key));
            sb.append(key);
            sb.append(":");
            sb.append(parameters.get(key));
            sb.append("\n");
        }

        Map<String, Object> result = new HashMap<>();

        result.put(propertiesName, sb.toString());


        // TODO: check credentials?
        // return new CreateServiceInstanceBindingResponse().withCredentials(credentials);
        return new ProvisionBindingCommand.Response(result);
    }

    @Override
    public DeleteBindingCommand.Response execute(DeleteBindingCommand deleteBindingCommand) throws ServiceBrokerException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTRY {}", deleteBindingCommand);
        }
        final DeleteBindingCommand.Response returnValue = new DeleteBindingCommand.Response();
        if (logger.isTraceEnabled()) {
            logger.trace("EXIT {}", returnValue);
        }

        return returnValue;
    }

    @Override
    public ProvisionServiceInstanceCommand.Response execute(ProvisionServiceInstanceCommand provisionServiceInstanceCommand) throws ServiceBrokerException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTRY {}", provisionServiceInstanceCommand);
        }
        ProvisionServiceInstanceCommand.Response returnValue = null;

        Map<? extends String, ?> parameters = provisionServiceInstanceCommand.getParameters();

        if (parameters != null && parameters.size() > 0) {

            StringBuffer sb = new StringBuffer();

            for (String key : parameters.keySet()) {
                sb.append(key);
                sb.append(":");
                sb.append(parameters.get(key));
                sb.append("\n");
            }

            logger.info("CreateServiceInstanceRequest contains parameters: " + sb.toString());
        } else {
            logger.info("CreateServiceInstanceRequest contains no parameters");
        }


        //return new CreateServiceInstanceResponse().withInstanceExisted(true).withDashboardUrl("https://dashboard:8080").withOperation("Create");
        try {
            returnValue = new ProvisionServiceInstanceCommand.Response( new URI("https://dashboard:8080"), new Operation("create"));
        } catch (URISyntaxException e) {
            logger.info("Exception: "+e.getMessage());
        }

        if (logger.isTraceEnabled()) {
            logger.trace("Exit {}", provisionServiceInstanceCommand);
        }

        return returnValue;
    }

    @Override
    public UpdateServiceInstanceCommand.Response execute(UpdateServiceInstanceCommand updateServiceInstanceCommand) throws ServiceBrokerException {
        return null;
    }

    @Override
    public DeleteServiceInstanceCommand.Response execute(DeleteServiceInstanceCommand deleteServiceInstanceCommand) throws ServiceBrokerException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTRY {}", deleteServiceInstanceCommand);
        }
        DeleteServiceInstanceCommand.Response returnValue = new DeleteServiceInstanceCommand.Response();

        if (logger.isTraceEnabled()) {
            logger.trace("EXIT {}", returnValue);
        }
        return returnValue;
    }

    @Override
    public ServiceInstance getServiceInstance(String s) throws ServiceBrokerException {
        return null;
    }

    @Override
    public Binding getBinding(String s, String s1) throws ServiceBrokerException {
        return null;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    public void logBind(String content) {
        logger.info("-- bind: " + content);
    }

    public String pullParameter(Map<? extends String, ?> parameters, String key) {
        Object nullDefault = null;
        return pullParameter(parameters, key, nullDefault);
    }
    public String pullParameter(Map<? extends String, ?> parameters, String key, Object defaultValue) {

        Object result = defaultValue;

        if(parameters == null || parameters.size() < 1) {
            logger.warn("pullParameters called with no parameter map");
        } else if(isEmpty(key)) {
            logger.warn("pullParameters called with no key");
        } else {
            if(parameters.containsKey(key)) {
                result = parameters.get(key);
            }
        }

        return (String) result;
    }

    boolean isEmpty(String str) {
        return (str == null || str.isEmpty());
    }

    public Catalog populateCatalog() {
        boolean bindable = true;
        List<Plan> plans = new ArrayList<Plan>();

        //plans.add( new Plan(UUID.randomUUID(),
        plans.add( new Plan("DynamoDBProductionPlan",
                "dynamodb-production-plan",
                "Production Plan for Dynamo Database Instance",
                null,
                true,
                bindable
        ));


        final Plan plan = new Plan("DynamoDBProductionPlan",
                "dynamodb-production-plan",
                "Production Plan for Dynamo Database Instance",
                null /* no metadata */,
                true /* free */,
                bindable /* pick up bindable information from the containing service */);


        final Service service = new Service("DynamoDB", "dynamodb-service", "AWS Dynamo Database Instance",
                null /* no tags */,
                null /* no requires */,
                bindable /* bindable */,
                null /* no metadata */,
                null /* no dashboardClient */,
                false /* not updatable */,
                Collections.singleton(plan));

        final Set<Service> services = new LinkedHashSet<>();
        services.add(service);
        Catalog catalog = new Catalog(services);
        return catalog;

    }
}
