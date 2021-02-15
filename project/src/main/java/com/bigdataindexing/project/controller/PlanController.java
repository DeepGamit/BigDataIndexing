package com.bigdataindexing.project.controller;

import com.bigdataindexing.project.exception.InvalidInputException;
import com.bigdataindexing.project.service.PlanService;
import org.apache.commons.digester.plugins.PluginInvalidInputException;
import org.apache.coyote.Response;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONTokener;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.json.JSONObject;

import javax.servlet.http.HttpServletResponse;
import java.awt.*;
import java.net.URI;
import java.net.URISyntaxException;

@RestController
public class PlanController {

    PlanService planService = new PlanService();


    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan")
    public ResponseEntity createPlan(@RequestBody  String jsonData, HttpServletResponse response) throws URISyntaxException {

        JSONObject jsonPlan = new JSONObject(new JSONTokener(jsonData));
        this.planService.validatePlan(jsonPlan);

        String objectID = this.planService.savePlan(jsonPlan, (String)jsonPlan.get("objectType"));

        String ETag = this.planService.generateETag(jsonPlan);

        return ResponseEntity.status(HttpStatus.CREATED).eTag(ETag).body("{\"objectId\": \"" + objectID + "\"}");
    }

    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectID}")
    public ResponseEntity getPlan(@PathVariable String objectID ){

        JSONObject jsonObject = this.planService.getPlan(objectID);

        return ResponseEntity.ok().body(jsonObject.toString());
    }


}
