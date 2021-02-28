package com.bigdataindexing.project.controller;


import com.bigdataindexing.project.service.ETagManager;
import com.bigdataindexing.project.service.PlanService;
import com.bigdataindexing.project.validator.JsonValidator;
import org.everit.json.schema.ValidationException;
import org.json.JSONTokener;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.json.JSONObject;
import javax.validation.Valid;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;


@RestController
public class PlanController {

    PlanService planService = new PlanService();
    ETagManager eTagManager = new ETagManager();
    JsonValidator jsonValidator = new JsonValidator();

    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan")
    public ResponseEntity createPlan(@Valid @RequestBody(required = false) String jsonData) throws URISyntaxException {

        if (jsonData == null || jsonData.isEmpty()){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                    body(new JSONObject().put("error", "Request body is Empty. Kindly provide the JSON").toString());
        }

        JSONObject jsonPlan = new JSONObject(new JSONTokener(jsonData));

        try {
            jsonValidator.validateJSON(jsonPlan);
        } catch(ValidationException ex){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                    body(new JSONObject().put("error",ex.getAllMessages()).toString());
        }

        if(this.planService.checkIfKeyExists((String) jsonPlan.get("objectId"))){
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(new JSONObject().put("message", "Plan already exists!!").toString());
        }

        String objectID = this.planService.savePlan(jsonPlan);
        String etag = this.eTagManager.getETag(jsonPlan);

        JSONObject response = new JSONObject();
        response.put("objectId", objectID);
        response.put("message", "Plan Created Successfully!!");

        return ResponseEntity.created(new URI("/plan/" +jsonPlan.get("objectId").toString())).eTag(etag)
                .body(response.toString());
    }

    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectID}")
    public ResponseEntity getPlan(@PathVariable String objectID, @RequestHeader HttpHeaders requestHeaders){

        if(!this.planService.checkIfKeyExists(objectID)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "No such object exists!!").toString());
        } else {

            JSONObject jsonObject = this.planService.getPlan(objectID);
            String etag = eTagManager.getETag(jsonObject);

            List<String> ifNotMatch;
            try{
                ifNotMatch = requestHeaders.getIfNoneMatch();
            } catch (Exception e){
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).
                        body(new JSONObject().put("error", "ETag value is invalid! If-None-Match value should be string.").toString());
            }

            if(!eTagManager.verifyETag(jsonObject, ifNotMatch)){
                return ResponseEntity.ok().eTag(etag).body(jsonObject.toString());
            } else {
                return  ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(etag).build();
            }
        }
    }

    @RequestMapping(method =  RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectID}")
    public ResponseEntity deletePlan(@PathVariable String objectID){

        if(!this.planService.checkIfKeyExists(objectID)){
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new JSONObject().put("message", "ObjectId does not exists!!").toString());
        }

        this.planService.deletePlan(objectID);
        return ResponseEntity.noContent().build();
    }
}
